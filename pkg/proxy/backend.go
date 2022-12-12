// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package proxy

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/recuffer/pkg/proxy/redis"
	"github.com/recuffer/pkg/utils/errors"
	"github.com/recuffer/pkg/utils/log"
	"github.com/recuffer/pkg/utils/math2"
	"github.com/recuffer/pkg/utils/sync2/atomic2"
)

const (
	stateConnected = iota + 1
	stateDataStale
)

var ctx = context.Background()

type BackendConn struct {
	stop sync.Once
	addr string

	// buffer为1024的channel
	input chan *Request
	retry struct {
		fails int
		delay Delay
	}
	state atomic2.Int64

	closed atomic2.Bool
	config *Config

	database int
	router   *Router
}

// 为每一个后端地址，创建一个连接池，用来处理啊moved、ask返回的错误请求
func (bc *BackendConn) NewMovedConnPool(addr string, config *Config) *MovedConnPool {
	return NewConnPool(&Options{
		PoolSize:           config.MovedConnPoolSize,            // 连接池最大连接
		MinIdleConns:       config.MovedConnPoolSize / 5,        // 连接池空闲连接
		PoolTimeout:        5 * time.Millisecond,                // 当前连接都处于繁忙状态，从连接池获取连接等待时长
		IdleCheckFrequency: config.BackendPingPeriod.Duration(), // 闲置连接检查的周期
		// IdleCheckFrequency: -1,                // 闲置连接检查的周期,-1停止检查
		IdleTimeout: 600 * time.Second, // 闲置超时,-1 表示取消闲置超时检查
		MaxConnAge:  8 * time.Hour,     // 连接存活时间长,从创建开始，超过这个时长则关闭
		addr:        addr,
		config:      config,
		// 新建moved pool时调用的创建的方法
		Dialer: func(ctx context.Context, addr string, config *Config) (*redis.Conn, error) {
			c, err := redis.DialTimeout(addr, time.Second*5,
				config.BackendRecvBufsize.AsInt(),
				config.BackendSendBufsize.AsInt())
			if err != nil {
				return nil, err
			}
			c.ReaderTimeout = config.BackendRecvTimeout.Duration()
			c.WriterTimeout = config.BackendSendTimeout.Duration()
			c.SetKeepAlivePeriod(config.BackendKeepAlivePeriod.Duration())
			if err := bc.verifyAuth(c, config.ProductAuth); err != nil {
				c.Close()
				return nil, err
			}

			// 	// 选择redis库
			if err := bc.selectDatabase(c, bc.database); err != nil {
				c.Close()
				return nil, err
			}
			// 从库提供读流量需要执行 readonly
			if err := bc.readOnly(c); err != nil {
				c.Close()
				return nil, err
			}
			return c, nil
		},
	})
}

func NewBackendConn(addr string, database int, config *Config, router *Router) *BackendConn {
	bc := &BackendConn{
		addr: addr, config: config, database: database,
	}
	bc.input = make(chan *Request, 1024)
	bc.retry.delay = &DelayExp2{
		Min: 50, Max: 5000,
		Unit: time.Millisecond,
	}
	// 遍历所有地址 生成处理连接moved请求的连接池
	if router.nodes != nil {
		for k := range router.nodes {
			if router.movedpool != nil {
				// router.movedpool[k] = CreatePool(k, config)
				if router.movedpool[k] == nil {
					router.movedpool[k] = bc.NewMovedConnPool(k, config)
				}
			}
		}
	} else {
		log.Warnf("newbackendconn init moved pool fail, nodes is nil, pleace check.")
	}
	bc.router = router
	go bc.run()

	return bc
}

func (bc *BackendConn) Addr() string {
	return bc.addr
}

func (bc *BackendConn) Close() {
	bc.stop.Do(func() {
		close(bc.input)
	})
	bc.closed.Set(true)
}

func (bc *BackendConn) GetMovedConn(addr string, config *Config) *MovedConn {
	if bc.router.movedpool != nil {
		movedconnpool := bc.router.movedpool[addr]
		if movedconnpool != nil {
			cn, err := movedconnpool.Get(ctx)
			if err != nil {
				log.Warnf("get moved conn is fail, err: %v.", err)
				return nil
			} else {
				return cn
			}
		} else {
			bc.router.movedpool[addr] = bc.NewMovedConnPool(addr, config)
			log.Warnf("get moved conn is fail, bc.router.movedpool[addr] is nil.")
			return nil
		}
	} else {
		log.Warnf("get moved conn is fail, router.movedpool is nil.")
		return nil
	}
}

func (bc *BackendConn) RelaseMovedConn(addr string, movedconn *MovedConn) {
	if bc.router.movedpool == nil {
		bc.router.movedpool = make(map[string]*MovedConnPool)
	}
	movedpool := bc.router.movedpool[addr]
	if movedpool != nil {
		movedpool.Put(ctx, movedconn)
	}
}

func (bc *BackendConn) RemoveMovedConn(addr string, movedconn *MovedConn) {
	movedpool := bc.router.movedpool[addr]
	if movedpool != nil {
		movedpool.Remove(ctx, movedconn, nil)
	} else {
		movedconn.con.Close()
	}
}

func (bc *BackendConn) IsConnected() bool {
	return bc.state.Int64() == stateConnected
}

func (bc *BackendConn) PushBack(r *Request) {
	if r.Batch != nil {
		r.Batch.Add(1)
	}
	// 放入管道
	bc.input <- r
}

func (bc *BackendConn) KeepAlive() bool {
	if len(bc.input) != 0 {
		return false
	}
	switch bc.state.Int64() {
	default:
		m := &Request{}
		m.Multi = []*redis.Resp{
			redis.NewBulkBytes([]byte("PING")),
		}
		bc.PushBack(m)

	case stateDataStale:
		m := &Request{}
		m.Multi = []*redis.Resp{
			redis.NewBulkBytes([]byte("INFO")),
		}
		m.Batch = &sync.WaitGroup{}
		bc.PushBack(m)

		keepAliveCallback <- func() {
			m.Batch.Wait()
			var err = func() error {
				if err := m.Err; err != nil {
					return err
				}
				switch resp := m.Resp; {
				case resp == nil:
					return ErrRespIsRequired
				case resp.IsError():
					return fmt.Errorf("bad info resp: %s", resp.Value)
				case resp.IsBulkBytes():
					var info = make(map[string]string)
					for _, line := range strings.Split(string(resp.Value), "\n") {
						kv := strings.SplitN(line, ":", 2)
						if len(kv) != 2 {
							continue
						}
						if key := strings.TrimSpace(kv[0]); key != "" {
							info[key] = strings.TrimSpace(kv[1])
						}
					}
					if info["master_link_status"] == "down" {
						return nil
					}
					if info["loading"] == "1" {
						return nil
					}
					if bc.state.CompareAndSwap(stateDataStale, stateConnected) {
						log.Warnf("backend conn [%p] to %s, db-%d state = Connected (keepalive)",
							bc, bc.addr, bc.database)
					}
					return nil
				default:
					return fmt.Errorf("bad info resp: should be string, but got %s", resp.Type)
				}
			}()
			if err != nil && bc.closed.IsFalse() {
				log.WarnErrorf(err, "backend conn [%p] to %s, db-%d recover from DataStale failed",
					bc, bc.addr, bc.database)
			}
		}
	}
	return true
}

var keepAliveCallback = make(chan func(), 128)

func init() {
	go func() {
		for fn := range keepAliveCallback {
			fn()
		}
	}()
}

func (bc *BackendConn) newBackendReader(round int, config *Config) (int64, *redis.Conn, chan<- *Request, error) {
	var bcState int64
	bcState = stateConnected
	// 创建与Redis的连接Conn
	c, err := redis.DialTimeout(bc.addr, time.Second*5,
		config.BackendRecvBufsize.AsInt(),
		config.BackendSendBufsize.AsInt())
	if err != nil {
		return bcState, nil, nil, err
	}
	c.ReaderTimeout = config.BackendRecvTimeout.Duration()
	c.WriterTimeout = config.BackendSendTimeout.Duration()
	c.SetKeepAlivePeriod(config.BackendKeepAlivePeriod.Duration())

	if err := bc.verifyAuth(c, config.ProductAuth); err != nil {
		c.Close()
		return bcState, nil, nil, err
	}

	// 选择redis库
	if err := bc.selectDatabase(c, bc.database); err != nil {
		c.Close()
		return bcState, nil, nil, err
	}
	// 从库提供读流量需要执行readonly，在连接新建完后，从库执行readonly命令
	if err := bc.readOnly(c); err != nil {
		c.Close()
		return bcState, nil, nil, err
	}
	// 判断是否是刚加进来的从库，防止在同步完成之前接收流量，造成请求结果为空
	bc_state, err := bc.ready(c)
	if err != nil {
		c.Close()
		return bcState, nil, nil, err
	} else {
		bcState = bc_state
	}
	tasks := make(chan *Request, config.BackendMaxPipeline)
	// 读取task中的请求，并将处理结果与之对应关联
	go bc.loopReader(tasks, c, round, config)

	return bcState, c, tasks, nil
}

func (bc *BackendConn) verifyAuth(c *redis.Conn, auth string) error {
	if auth == "" {
		return nil
	}

	multi := []*redis.Resp{
		redis.NewBulkBytes([]byte("AUTH")),
		redis.NewBulkBytes([]byte(auth)),
	}

	if err := c.EncodeMultiBulk(multi, true); err != nil {
		return err
	}

	resp, err := c.Decode()
	switch {
	case err != nil:
		return err
	case resp == nil:
		return ErrRespIsRequired
	case resp.IsError():
		return fmt.Errorf("error resp: %s", resp.Value)
	case resp.IsString():
		return nil
	default:
		return fmt.Errorf("error resp: should be string, but got %s", resp.Type)
	}
}

func (bc *BackendConn) selectDatabase(c *redis.Conn, database int) error {
	if database == 0 {
		return nil
	}

	multi := []*redis.Resp{
		redis.NewBulkBytes([]byte("SELECT")),
		redis.NewBulkBytes([]byte(strconv.Itoa(database))),
	}

	if err := c.EncodeMultiBulk(multi, true); err != nil {
		return err
	}

	resp, err := c.Decode()
	switch {
	case err != nil:
		return err
	case resp == nil:
		return ErrRespIsRequired
	case resp.IsError():
		return fmt.Errorf("error resp: %s", resp.Value)
	case resp.IsString():
		return nil
	default:
		return fmt.Errorf("error resp: should be string, but got %s", resp.Type)
	}
}

// proxy新建后端的连接时，执行readonly命令
func (bc *BackendConn) readOnly(c *redis.Conn) error {
	// 先判断实例是否为slave角色，是的话需要执行readonly
	info_multi := []*redis.Resp{
		redis.NewBulkBytes([]byte("INFO")),
	}
	if err := c.EncodeMultiBulk(info_multi, true); err != nil {
		return err
	}
	info_resp, info_err := c.Decode()
	switch {
	case info_err != nil:
		return info_err
	case info_resp == nil:
		return ErrRespIsRequired
	case info_resp.IsError():
		return fmt.Errorf("error resp: %s", info_resp.Value)
	case info_resp.IsBulkBytes():
		var info = make(map[string]string)
		for _, line := range strings.Split(string(info_resp.Value), "\n") {
			kv := strings.SplitN(line, ":", 2)
			if len(kv) != 2 {
				continue
			}
			if key := strings.TrimSpace(kv[0]); key != "" {
				info[key] = strings.TrimSpace(kv[1])
			}
		}
		if info["role"] == "slave" {
			multi := []*redis.Resp{
				redis.NewBulkBytes([]byte("READONLY")),
			}

			if err := c.EncodeMultiBulk(multi, true); err != nil {
				return err
			}

			resp, err := c.Decode()
			switch {
			case err != nil:
				return err
			case resp == nil:
				return ErrRespIsRequired
			case resp.IsError():
				return fmt.Errorf("error resp: %s", resp.Value)
			case resp.IsString():
				return nil
			default:
				return fmt.Errorf("error resp: readonly is fail: %s", resp.Type)
			}
		} else {
			return nil
		}
	default:
		return fmt.Errorf("bad info resp: should be string, but got %s", info_resp.Type)
	}
}

// 处理ask请求的时候，需要先发送asking命令
func (bc *BackendConn) asking(c *redis.Conn) error {
	// 在会话中执行asking命令
	multi := []*redis.Resp{
		redis.NewBulkBytes([]byte("ASKING")),
	}

	if err := c.EncodeMultiBulk(multi, true); err != nil {
		return err
	}

	resp, err := c.Decode()
	switch {
	case err != nil:
		return err
	case resp == nil:
		return ErrRespIsRequired
	case resp.IsError():
		return fmt.Errorf("error resp: %s", resp.Value)
	case resp.IsString():
		return nil
	default:
		return fmt.Errorf("error resp: asking is fail: %s", resp.Type)
	}
}

// proxy新建后端连接的时候，先判断节点是否已准备好(主从复制建立成功，数据已loading完成，进入增量复制阶段)
func (bc *BackendConn) ready(c *redis.Conn) (int64, error) {
	var bcState int64
	bcState = stateConnected
	// 先判断实例是否为slave角色，是的话需要执行readonly
	info_multi := []*redis.Resp{
		redis.NewBulkBytes([]byte("INFO")),
	}
	if err := c.EncodeMultiBulk(info_multi, true); err != nil {
		return bcState, err
	}
	info_resp, info_err := c.Decode()
	switch {
	case info_err != nil:
		return bcState, info_err
	case info_resp == nil:
		return bcState, ErrRespIsRequired
	case info_resp.IsError():
		return bcState, fmt.Errorf("error resp: %s", info_resp.Value)
	case info_resp.IsBulkBytes():
		var info = make(map[string]string)
		for _, line := range strings.Split(string(info_resp.Value), "\n") {
			kv := strings.SplitN(line, ":", 2)
			if len(kv) != 2 {
				continue
			}
			if key := strings.TrimSpace(kv[0]); key != "" {
				info[key] = strings.TrimSpace(kv[1])
			}
		}
		if info["role"] == "slave" {
			if info["master_link_status"] == "down" {
				bcState = stateDataStale
			}
			if info["loading"] == "1" {
				bcState = stateDataStale
			}
		}
		return bcState, nil
	default:
		return bcState, fmt.Errorf("bad info resp: should be string, but got %s", info_resp.Type)
	}
}

func (bc *BackendConn) setResponse(r *Request, resp *redis.Resp, err error) error {
	r.Resp, r.Err = resp, err
	if r.Group != nil {
		r.Group.Done()
	}
	if r.Batch != nil {
		r.Batch.Done()
	}
	return err
}

var (
	ErrBackendConnReset = errors.New("backend conn reset")
	ErrRequestIsBroken  = errors.New("request is broken")
)

// BackendConn负责实际对redis请求进行处理。在fillSlot的时候，主要目的就是给slot填充backend.bc（实际上是sharedBackendConn)。
// 从models.slot得到BackendAddr和MigrateFrom的地址addr，根据这个addr,首先从proxy.Router的primary: sharedBackendConnPool
// 中取sharedBackendConn，如果没有获取到，就新建sharedBackendConn再放回sharedBackendConnPool。
//
// 创建sharedBackendConn的过程中启动了两个goroutine，分别是loopWriter和loopReader，loopWriter负责从backendConn.input中
// 取出请求并发送，loopReader负责遍历所有请求，从redis.Conn中解码得到resp并设置为相关的请求的属性，这样每一个请求及其结果就关联起来了。
func (bc *BackendConn) run() {
	log.Warnf("backend conn [%p] to %s, db-%d start service",
		bc, bc.addr, bc.database)
	for round := 0; bc.closed.IsFalse(); round++ {
		log.Warnf("backend conn [%p] to %s, db-%d round-[%d]",
			bc, bc.addr, bc.database, round)
		if err := bc.loopWriter(round); err != nil {
			bc.delayBeforeRetry()
		}
	}
	log.Warnf("backend conn [%p] to %s, db-%d stop and exit",
		bc, bc.addr, bc.database)
}

var (
	errRespMasterDown = []byte("MASTERDOWN")
	errRespLoading    = []byte("LOADING")
	errMovedError     = []byte("MOVED ")
	errAskError       = []byte("ASK ")
)

// 判断返回的信息是否包含moved、ask请求错误
func isMovedError(move []byte) (addr string) {
	s := BytesToStr(move)
	ind := strings.LastIndex(s, " ")
	if ind == -1 {
		return ""
	}
	addr = s[ind+1:]
	return
}

// 处理请求返回moved及ask错误
func (bc *BackendConn) loopMoved(addr string, movedconn *MovedConn, r *Request, movedResp *redis.Resp) (v *redis.Resp, err error) {
	defer func() {
		if err != nil {
			bc.RemoveMovedConn(addr, movedconn)
			log.Warnf("loop moved exec request error, %v, remove movedconn", err)
		}
	}()
	// 判断假如是ask请求错误，再调用命令前，先执行asking命令
	if bytes.HasPrefix(movedResp.Value, errAskError) {
		if err := bc.asking(movedconn.con); err != nil {
			log.Warnf("loop moved exec asking error: %v", err)
		}
	}
	if err := movedconn.con.EncodeMultiBulk(r.Multi, true); err != nil {
		log.Warnf("loop moved exec request error, %v", err)
		return nil, err
	}
	resp, err := movedconn.con.Decode()
	if err != nil {
		log.Warnf("loop moved decode request error, %v", err)
		return nil, err
	}
	bc.RelaseMovedConn(addr, movedconn)
	return resp, nil

}

func (bc *BackendConn) loopReader(tasks <-chan *Request, c *redis.Conn, round int, config *Config) (err error) {
	// 从连接中取完所有请求并setResponse之后，连接就会关闭
	defer func() {
		c.Close()
		for r := range tasks {
			bc.setResponse(r, nil, ErrBackendConnReset)
		}
		log.WarnErrorf(err, "backend conn [%p] to %s, db-%d reader-[%d] exit",
			bc, bc.addr, bc.database, round)
	}()
	// 遍历tasks，此时的r是所有的请求
	for r := range tasks {
		var resp *redis.Resp
		var err error
		// 从redis.Conn中解码得到处理结果
		resp, err = c.Decode()
		if err != nil {
			return bc.setResponse(r, nil, fmt.Errorf("backend conn failure, %s", err))
		}
		if resp != nil && resp.IsError() {
			switch {
			case bytes.HasPrefix(resp.Value, errRespMasterDown):
				if bc.state.CompareAndSwap(stateConnected, stateDataStale) {
					log.Warnf("backend conn [%p] to %s, db-%d state = DataStale, caused by 'MASTERDOWN'",
						bc, bc.addr, bc.database)
				}
			case bytes.HasPrefix(resp.Value, errRespLoading):
				if bc.state.CompareAndSwap(stateConnected, stateDataStale) {
					log.Warnf("backend conn [%p] to %s, db-%d state = DataStale, caused by 'LOADING'",
						bc, bc.addr, bc.database)
				}
			// 处理moved 和 asked 返回结果
			case bytes.HasPrefix(resp.Value, errMovedError) || bytes.HasPrefix(resp.Value, errAskError):
				var movedResp *redis.Resp
				movedResp = resp
				// 这里循环三次处理moved、ask请求，防止信息没同步造成的失败，超过3次就会直接将结果返回，这个值时经过
				// 测试发现比较合理的值
				for i := 0; i < 3; i++ {
					addr := isMovedError(movedResp.Value)
					movedcon := bc.GetMovedConn(addr, config)
					if movedcon == nil {
						log.Warnf("moved request get movedcon failure, movedcon is nil.")
						continue
					}
					resp, err = bc.loopMoved(addr, movedcon, r, movedResp)
					if err != nil {
						return bc.setResponse(r, nil, fmt.Errorf("moved request failure, %s", err))
					}
					if resp != nil && resp.IsError() {
						switch {
						case bytes.HasPrefix(resp.Value, errRespMasterDown):
							log.Warnf("loop reader moved request to %s db-%d fail, caused by 'MASTERDOWN'", addr, bc.database)
						case bytes.HasPrefix(resp.Value, errRespLoading):
							log.Warnf("loop reader moved request to %s db-%d fail, caused by 'LOADING'", addr, bc.database)
						// 处理moved 和 asked 返回结果
						case bytes.HasPrefix(resp.Value, errMovedError) || bytes.HasPrefix(resp.Value, errAskError):
							movedResp = resp
						}
					} else {
						break
					}
				}
				// if err != nil {
				// 	return bc.setResponse(r, nil, fmt.Errorf("moved request failure, %s", err))
				// }
			}
		}
		// 请求结果设置为请求的属性
		bc.setResponse(r, resp, nil)
	}
	return nil
}

func (bc *BackendConn) delayBeforeRetry() {
	bc.retry.fails += 1
	if bc.retry.fails <= 10 {
		return
	}
	timeout := bc.retry.delay.After()
	for bc.closed.IsFalse() {
		select {
		case <-timeout:
			return
		case r, ok := <-bc.input:
			if !ok {
				return
			}
			bc.setResponse(r, nil, ErrBackendConnReset)
		}
	}
}

func (bc *BackendConn) loopWriter(round int) (err error) {
	// 如果因为某种原因退出，还有input没来得及处理，就返回错误。
	defer func() {
		for i := len(bc.input); i != 0; i-- {
			r := <-bc.input
			bc.setResponse(r, nil, ErrBackendConnReset)
		}
		log.WarnErrorf(err, "backend conn [%p] to %s, db-%d writer-[%d] exit",
			bc, bc.addr, bc.database, round)
	}()
	// 这个方法内启动了loopReader
	bcState, c, tasks, err := bc.newBackendReader(round, bc.config)
	if err != nil {
		return err
	}
	defer close(tasks)

	defer bc.state.Set(0)

	bc.state.Set(bcState)
	bc.retry.fails = 0
	bc.retry.delay.Reset()

	p := c.FlushEncoder()
	p.MaxInterval = time.Millisecond
	p.MaxBuffered = cap(tasks) / 2

	// 循环从BackendConn的input这个channel取redis请求
	for r := range bc.input {
		if r.IsReadOnly() && r.IsBroken() {
			bc.setResponse(r, nil, ErrRequestIsBroken)
			continue
		}
		// 将请求取出并发送给codis-server
		if err := p.EncodeMultiBulk(r.Multi); err != nil {
			return bc.setResponse(r, nil, fmt.Errorf("backend conn failure, %s", err))
		}
		if err := p.Flush(len(bc.input) == 0); err != nil {
			return bc.setResponse(r, nil, fmt.Errorf("backend conn failure, %s", err))
		} else {
			// 所有的请求写入tasks这个channel
			tasks <- r
		}
	}
	return nil
}

type sharedBackendConn struct {
	addr string
	host []byte
	port []byte

	// 所属的池
	owner *sharedBackendConnPool
	conns [][]*BackendConn

	single []*BackendConn

	// 当前sharedBackendConn的引用计数，非正数的时候说明处于关闭状态，每增加一个引用就加一。
	refcnt int
}

func newSharedBackendConn(addr string, pool *sharedBackendConnPool, router *Router) *sharedBackendConn {
	// 拆分ip和端口号
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		log.ErrorErrorf(err, "split host-port failed, address = %s", addr)
	}
	s := &sharedBackendConn{
		addr: addr,
		host: []byte(host), port: []byte(port),
	}
	// 确认新建的sharedBackendConn所属的pool
	s.owner = pool
	// len和cap都默认为16的二维切片
	s.conns = make([][]*BackendConn, pool.config.BackendNumberDatabases)
	// range用一个参数遍历二维切片，database是0到15
	for database := range s.conns {
		// len和cap都默认为1的一维切片
		parallel := make([]*BackendConn, pool.parallel)
		// 只有parallel[0]
		for i := range parallel {
			parallel[i] = NewBackendConn(addr, database, pool.config, router)
		}
		s.conns[database] = parallel
	}
	if pool.parallel == 1 {
		s.single = make([]*BackendConn, len(s.conns))
		for database := range s.conns {
			s.single[database] = s.conns[database][0]
		}
	}
	// 新建之后，这个SharedBackendConn的引用次数就置为1.
	s.refcnt = 1
	return s
}

func (s *sharedBackendConn) Addr() string {
	if s == nil {
		return ""
	}
	return s.addr
}

func (s *sharedBackendConn) Release() {
	if s == nil {
		return
	}
	if s.refcnt <= 0 {
		log.Panicf("shared backend conn has been closed, close too many times")
	} else {
		s.refcnt--
	}
	if s.refcnt != 0 {
		return
	}
	for _, parallel := range s.conns {
		for _, bc := range parallel {
			bc.Close()
		}
	}
	delete(s.owner.pool, s.addr)
}

func (s *sharedBackendConn) Retain() *sharedBackendConn {
	if s == nil {
		return nil
	}
	if s.refcnt <= 0 {
		log.Panicf("shared backend conn has been closed")
	} else {
		s.refcnt++
	}
	return s
}

func (s *sharedBackendConn) KeepAlive() {
	if s == nil {
		return
	}
	for _, parallel := range s.conns {
		for _, bc := range parallel {
			bc.KeepAlive()
		}
	}
}

func (s *sharedBackendConn) BackendConn(database int32, seed uint, must bool) *BackendConn {
	if s == nil {
		return nil
	}

	if s.single != nil {
		bc := s.single[database]
		if must || bc.IsConnected() {
			return bc
		}
		return nil
	}

	var parallel = s.conns[database]

	var i = seed
	for range parallel {
		i = (i + 1) % uint(len(parallel))
		if bc := parallel[i]; bc.IsConnected() {
			return bc
		}
	}
	if !must {
		return nil
	}
	return parallel[0]
}

type sharedBackendConnPool struct {
	// 从启动配置文件参数封装的config
	config   *Config
	parallel int

	pool map[string]*sharedBackendConn
}

func newSharedBackendConnPool(config *Config, parallel int) *sharedBackendConnPool {
	p := &sharedBackendConnPool{
		config: config, parallel: math2.MaxInt(1, parallel),
	}
	p.pool = make(map[string]*sharedBackendConn)
	return p
}

func (p *sharedBackendConnPool) KeepAlive() {
	for _, bc := range p.pool {
		bc.KeepAlive()
	}
}

func (p *sharedBackendConnPool) Get(addr string) *sharedBackendConn {
	return p.pool[addr]
}

// 先从连接池p.pool[addr]中优先取，如果能取出就直接取，缓存中为空的话就新建。
func (p *sharedBackendConnPool) Retain(addr string, router *Router) *sharedBackendConn {
	// 首先从pool中直接取，取到的话，引用计数加一
	if bc := p.pool[addr]; bc != nil {
		return bc.Retain()
	} else {
		// 取不到就新建，然后放到pool里面
		bc = newSharedBackendConn(addr, p, router)
		p.pool[addr] = bc
		return bc
	}
}
