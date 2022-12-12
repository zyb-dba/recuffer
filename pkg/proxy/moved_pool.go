package proxy

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/recuffer/pkg/proxy/redis"
	"github.com/recuffer/pkg/utils/errors"
)

var (
	// ErrClosed performs any operation on the closed client will return this error.
	ErrClosed = errors.New("redis: client is closed")

	// ErrPoolTimeout timed out waiting to get a connection from the connection pool.
	ErrPoolTimeout = errors.New("redis: connection pool timeout")
)

var timers = sync.Pool{
	New: func() interface{} {
		t := time.NewTimer(time.Hour)
		t.Stop()
		return t
	},
}

type Pooler interface {
	NewConn(context.Context) (*MovedConn, error)
	CloseConn(*MovedConn) error

	Get(context.Context) (*MovedConn, error)
	Put(context.Context, *MovedConn)
	Remove(context.Context, *MovedConn, error)

	// Len() int
	// IdleLen() int
	// Stats() *Stats

	Close() error
}

type Options struct {
	Dialer  func(ctx context.Context, addr string, config *Config) (*redis.Conn, error)
	OnClose func(*MovedConn) error

	PoolSize     int
	MinIdleConns int

	PoolTimeout        time.Duration // 当前连接都处于繁忙状态，从连接池获取连接等待时长
	IdleCheckFrequency time.Duration // 闲置连接检查的周期
	IdleTimeout        time.Duration // 闲置超时,-1 表示取消闲置超时检查
	MaxConnAge         time.Duration // 连接存活时间长,从创建开始，超过这个时长则关闭

	addr   string
	config *Config
}

type lastDialErrorWrap struct {
	err error
}

type MovedConn struct {
	con       *redis.Conn
	Inited    bool      // 是否完成初始化
	pooled    bool      // 是否放进连接池
	createdAt time.Time // 创建时间
	usedAt    int64     // 使用时间，atomic
}

func NewConn(Conn *redis.Conn) *MovedConn {
	cn := &MovedConn{
		con:       Conn,
		createdAt: time.Now(),
	}
	cn.SetUsedAt(time.Now())
	return cn
}

func (mcn *MovedConn) UsedAt() time.Time {
	unix := atomic.LoadInt64(&mcn.usedAt)
	return time.Unix(unix, 0)
}

func (mcn *MovedConn) SetUsedAt(tm time.Time) {
	atomic.StoreInt64(&mcn.usedAt, tm.Unix())
}

type MovedConnPool struct {
	opt *Options

	dialErrorsNum uint32 // atomic

	lastDialError atomic.Value

	queue chan struct{}

	connsMu      sync.Mutex
	conns        []*MovedConn
	idleConns    []*MovedConn
	poolSize     int
	idleConnsLen int

	// stats Stats

	_closed  uint32 // atomic
	closedCh chan struct{}
}

func NewConnPool(opt *Options) *MovedConnPool {
	p := &MovedConnPool{
		opt: opt,

		queue:     make(chan struct{}, opt.PoolSize),
		conns:     make([]*MovedConn, 0, opt.PoolSize),
		idleConns: make([]*MovedConn, 0, opt.PoolSize),
		closedCh:  make(chan struct{}),
	}

	p.connsMu.Lock()
	p.checkMinIdleConns()
	p.connsMu.Unlock()

	if opt.IdleTimeout > 0 && opt.IdleCheckFrequency > 0 {
		go p.reaper(opt.IdleCheckFrequency)
	}

	return p
}

func (p *MovedConnPool) checkMinIdleConns() {
	if p.opt.MinIdleConns == 0 {
		return
	}
	for p.poolSize < p.opt.PoolSize && p.idleConnsLen < p.opt.MinIdleConns {
		p.poolSize++
		p.idleConnsLen++
		go func() {
			err := p.addIdleConn()
			if err != nil {
				p.connsMu.Lock()
				p.poolSize--
				p.idleConnsLen--
				p.connsMu.Unlock()
			}
		}()
	}
}

func (p *MovedConnPool) addIdleConn() error {
	cn, err := p.dialConn(context.TODO(), true)
	if err != nil {
		return err
	}

	p.connsMu.Lock()
	p.conns = append(p.conns, cn)
	p.idleConns = append(p.idleConns, cn)
	p.connsMu.Unlock()
	return nil
}

func (p *MovedConnPool) NewConn(ctx context.Context) (*MovedConn, error) {
	return p.newConn(ctx, false)
}

func (p *MovedConnPool) newConn(ctx context.Context, pooled bool) (*MovedConn, error) {
	cn, err := p.dialConn(ctx, pooled)
	if err != nil {
		return nil, err
	}

	p.connsMu.Lock()
	p.conns = append(p.conns, cn)
	if pooled {
		// If pool is full remove the cn on next Put.
		if p.poolSize >= p.opt.PoolSize {
			cn.pooled = false
		} else {
			p.poolSize++
		}
	}
	p.connsMu.Unlock()

	return cn, nil
}

func (p *MovedConnPool) dialConn(ctx context.Context, pooled bool) (*MovedConn, error) {
	if p.closed() {
		return nil, ErrClosed
	}

	if atomic.LoadUint32(&p.dialErrorsNum) >= uint32(p.opt.PoolSize) {
		return nil, p.getLastDialError()
	}

	conn, err := p.opt.Dialer(ctx, p.opt.addr, p.opt.config)
	if err != nil {
		p.setLastDialError(err)
		if atomic.AddUint32(&p.dialErrorsNum, 1) == uint32(p.opt.PoolSize) {
			go p.tryDial()
		}
		return nil, err
	}

	// internal.NewConnectionsCounter.Add(ctx, 1)
	cn := NewConn(conn)
	cn.pooled = pooled
	return cn, nil
}

func (p *MovedConnPool) tryDial() {
	for {
		if p.closed() {
			return
		}

		conn, err := p.opt.Dialer(context.Background(), p.opt.addr, p.opt.config)
		if err != nil {
			p.setLastDialError(err)
			time.Sleep(time.Second)
			continue
		}

		atomic.StoreUint32(&p.dialErrorsNum, 0)
		_ = conn.Close()
		return
	}
}

func (p *MovedConnPool) setLastDialError(err error) {
	p.lastDialError.Store(&lastDialErrorWrap{err: err})
}

func (p *MovedConnPool) getLastDialError() error {
	err, _ := p.lastDialError.Load().(*lastDialErrorWrap)
	if err != nil {
		return err.err
	}
	return nil
}

// Get returns existed connection from the pool or creates a new one.
func (p *MovedConnPool) Get(ctx context.Context) (*MovedConn, error) {
	if p.closed() {
		return nil, ErrClosed
	}

	if err := p.waitTurn(ctx); err != nil {
		return nil, err
	}

	for {
		p.connsMu.Lock()
		cn := p.popIdle()
		p.connsMu.Unlock()

		if cn == nil {
			break
		}

		if p.isStaleConn(cn) {
			_ = p.CloseConn(cn)
			continue
		}

		// atomic.AddUint32(&p.stats.Hits, 1)
		return cn, nil
	}

	// atomic.AddUint32(&p.stats.Misses, 1)

	newcn, err := p.newConn(ctx, true)
	if err != nil {
		p.freeTurn()
		return nil, err
	}

	return newcn, nil
}

func (p *MovedConnPool) getTurn() {
	p.queue <- struct{}{}
}

func (p *MovedConnPool) waitTurn(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	select {
	case p.queue <- struct{}{}:
		return nil
	default:
	}

	timer := timers.Get().(*time.Timer)
	timer.Reset(p.opt.PoolTimeout)

	select {
	case <-ctx.Done():
		if !timer.Stop() {
			<-timer.C
		}
		timers.Put(timer)
		return ctx.Err()
	case p.queue <- struct{}{}:
		if !timer.Stop() {
			<-timer.C
		}
		timers.Put(timer)
		return nil
	case <-timer.C:
		timers.Put(timer)
		// atomic.AddUint32(&p.stats.Timeouts, 1)
		return ErrPoolTimeout
	}
}

func (p *MovedConnPool) freeTurn() {
	<-p.queue
}

func (p *MovedConnPool) popIdle() *MovedConn {
	if len(p.idleConns) == 0 {
		return nil
	}

	idx := len(p.idleConns) - 1
	cn := p.idleConns[idx]
	p.idleConns = p.idleConns[:idx]
	p.idleConnsLen--
	p.checkMinIdleConns()
	return cn
}

func (p *MovedConnPool) Put(ctx context.Context, cn *MovedConn) {
	// if cn.rd.Buffered() > 0 {
	// 	internal.Logger.Printf(ctx, "Conn has unread data")
	// 	p.Remove(ctx, cn, BadConnError{})
	// 	return
	// }

	if !cn.pooled {
		p.Remove(ctx, cn, nil)
		return
	}

	p.connsMu.Lock()
	p.idleConns = append(p.idleConns, cn)
	p.idleConnsLen++
	p.connsMu.Unlock()
	p.freeTurn()
}

func (p *MovedConnPool) Remove(ctx context.Context, cn *MovedConn, reason error) {
	p.removeConnWithLock(cn)
	p.freeTurn()
	_ = p.closeConn(cn)
}

func (p *MovedConnPool) CloseConn(cn *MovedConn) error {
	p.removeConnWithLock(cn)
	return p.closeConn(cn)
}

func (p *MovedConnPool) removeConnWithLock(cn *MovedConn) {
	p.connsMu.Lock()
	p.removeConn(cn)
	p.connsMu.Unlock()
}

func (p *MovedConnPool) removeConn(cn *MovedConn) {
	for i, c := range p.conns {
		if c == cn {
			p.conns = append(p.conns[:i], p.conns[i+1:]...)
			if cn.pooled {
				p.poolSize--
				p.checkMinIdleConns()
			}
			return
		}
	}
}

func (p *MovedConnPool) closeConn(cn *MovedConn) error {
	if p.opt.OnClose != nil {
		_ = p.opt.OnClose(cn)
	}
	return cn.con.Close()
}

func (p *MovedConnPool) closed() bool {
	return atomic.LoadUint32(&p._closed) == 1
}

func (p *MovedConnPool) Filter(fn func(*MovedConn) bool) error {
	p.connsMu.Lock()
	defer p.connsMu.Unlock()

	var firstErr error
	for _, cn := range p.conns {
		if fn(cn) {
			if err := p.closeConn(cn); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

func (p *MovedConnPool) Close() error {
	if !atomic.CompareAndSwapUint32(&p._closed, 0, 1) {
		return ErrClosed
	}
	close(p.closedCh)

	var firstErr error
	p.connsMu.Lock()
	for _, cn := range p.conns {
		if err := p.closeConn(cn); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	p.conns = nil
	p.poolSize = 0
	p.idleConns = nil
	p.idleConnsLen = 0
	p.connsMu.Unlock()

	return firstErr
}

func (p *MovedConnPool) reaper(frequency time.Duration) {
	ticker := time.NewTicker(frequency)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// It is possible that ticker and closedCh arrive together,
			// and select pseudo-randomly pick ticker case, we double
			// check here to prevent being executed after closed.
			if p.closed() {
				return
			}
			_, err := p.ReapStaleConns()
			if err != nil {
				// internal.Logger.Printf(context.Background(), "ReapStaleConns failed: %s", err)
				continue
			}
		case <-p.closedCh:
			return
		}
	}
}

func (p *MovedConnPool) ReapStaleConns() (int, error) {
	var n int
	for {
		p.getTurn()

		p.connsMu.Lock()
		cn := p.reapStaleConn()
		p.connsMu.Unlock()

		p.freeTurn()

		if cn != nil {
			_ = p.closeConn(cn)
			n++
		} else {
			break
		}
	}
	// atomic.AddUint32(&p.stats.StaleConns, uint32(n))
	return n, nil
}

func (p *MovedConnPool) reapStaleConn() *MovedConn {
	if len(p.idleConns) == 0 {
		return nil
	}

	cn := p.idleConns[0]
	if !p.isStaleConn(cn) {
		return nil
	}

	p.idleConns = append(p.idleConns[:0], p.idleConns[1:]...)
	p.idleConnsLen--
	p.removeConn(cn)

	return cn
}

func (p *MovedConnPool) isStaleConn(cn *MovedConn) bool {
	if p.opt.IdleTimeout == 0 && p.opt.MaxConnAge == 0 {
		return false
	}

	now := time.Now()
	if p.opt.IdleTimeout > 0 && now.Sub(cn.UsedAt()) >= p.opt.IdleTimeout {
		return true
	}
	if p.opt.MaxConnAge > 0 && now.Sub(cn.createdAt) >= p.opt.MaxConnAge {
		return true
	}

	return false
}
