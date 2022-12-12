// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package redis

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"golang.org/x/net/context"

	"github.com/recuffer/pkg/utils/errors"
	"github.com/recuffer/pkg/utils/sync2/atomic2"

	redigo "github.com/garyburd/redigo/redis"
)

type Sentinel struct {
	context.Context
	Cancel context.CancelFunc

	Product, Auth string

	LogFunc func(format string, args ...interface{})
	ErrFunc func(err error, format string, args ...interface{})
}

func NewSentinel(product, auth string) *Sentinel {
	s := &Sentinel{Product: product, Auth: auth}
	s.Context, s.Cancel = context.WithCancel(context.Background())
	return s
}

func (s *Sentinel) IsCanceled() bool {
	select {
	case <-s.Context.Done():
		return true
	default:
		return false
	}
}

func (s *Sentinel) NodeName(gid int) string {
	return fmt.Sprintf("%s-%d", s.Product, gid)
}

func (s *Sentinel) isSameProduct(name string) (gid int, _ bool) {
	if !strings.HasPrefix(name, s.Product) {
		return 0, false
	}
	var suffix = name[len(s.Product):]
	if len(suffix) <= 1 || suffix[0] != '-' {
		return 0, false
	}
	n, err := strconv.Atoi(suffix[1:])
	if err != nil {
		return 0, false
	}
	return n, true
}

func (s *Sentinel) printf(format string, args ...interface{}) {
	if s.LogFunc != nil {
		s.LogFunc(format, args...)
	}
}

func (s *Sentinel) errorf(err error, format string, args ...interface{}) {
	if s.ErrFunc != nil {
		s.ErrFunc(err, format, args...)
	}
}

func (s *Sentinel) do(sentinel string, timeout time.Duration,
	fn func(client *Client) error) error {
	c, err := NewClientNoAuth(sentinel, timeout)
	if err != nil {
		return err
	}
	defer c.Close()
	return fn(c)
}

func (s *Sentinel) dispatch(ctx context.Context, sentinel string, timeout time.Duration,
	fn func(client *Client) error) error {
	c, err := NewClientNoAuth(sentinel, timeout)
	if err != nil {
		return err
	}
	defer c.Close()

	var exit = make(chan error, 1)

	go func() {
		exit <- fn(c)
	}()

	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	case err := <-exit:
		return err
	}
}

func (s *Sentinel) subscribeCommand(client *Client, sentinel string,
	onSubscribed func()) error {
	defer func() {
		client.Close()
	}()
	var channels = []interface{}{"+switch-master"}
	go func() {
		client.Send("SUBSCRIBE", channels...)
		client.Flush()
	}()
	for _, sub := range channels {
		values, err := redigo.Values(client.Receive())
		if err != nil {
			return errors.Trace(err)
		} else if len(values) != 3 {
			return errors.Errorf("invalid response = %v", values)
		}
		s, err := redigo.Strings(values[:2], nil)
		if err != nil || s[0] != "subscribe" || s[1] != sub.(string) {
			return errors.Errorf("invalid response = %v", values)
		}
	}
	onSubscribed()
	for {
		values, err := redigo.Values(client.Receive())
		if err != nil {
			return errors.Trace(err)
		} else if len(values) < 2 {
			return errors.Errorf("invalid response = %v", values)
		}
		message, err := redigo.Strings(values, nil)
		if err != nil || message[0] != "message" {
			return errors.Errorf("invalid response = %v", values)
		}
		s.printf("sentinel-[%s] subscribe event %v", sentinel, message)

		// 从订阅的channel中读取消息
		switch message[1] {
		case "+switch-master":
			if len(message) != 3 {
				return errors.Errorf("invalid response = %v", values)
			}
			var params = strings.SplitN(message[2], " ", 2)
			if len(params) != 2 {
				return errors.Errorf("invalid response = %v", values)
			}
			_, yes := s.isSameProduct(params[0])
			if yes {
				return nil
			}
		}
	}
}

// 订阅"+switch-master"成功则返回true
func (s *Sentinel) subscribeDispatch(ctx context.Context, sentinel string, timeout time.Duration,
	onSubscribed func()) (bool, error) {
	var err = s.dispatch(ctx, sentinel, timeout, func(c *Client) error {
		return s.subscribeCommand(c, sentinel, onSubscribed)
	})
	if err != nil {
		switch errors.Cause(err) {
		case context.Canceled, context.DeadlineExceeded:
			return false, nil
		default:
			return false, err
		}
	}
	return true, nil
}

// Subscribe是让sentinel订阅名为"+switch-master"的channel，并从这个channel中读取主从切换的信息。
// 将订阅成功与否写到results中，最后再遍历results
func (s *Sentinel) Subscribe(sentinels []string, timeout time.Duration, onMajoritySubscribed func()) bool {
	cntx, cancel := context.WithTimeout(s.Context, timeout)
	defer cancel()

	timeout += time.Second * 5
	results := make(chan bool, len(sentinels))

	// 集群中sentinel数量的一半以上
	var majority = 1 + len(sentinels)/2

	var subscribed atomic2.Int64
	for i := range sentinels {
		go func(sentinel string) {
			notified, err := s.subscribeDispatch(cntx, sentinel, timeout, func() {
				// 订阅成功触发的逻辑
				if subscribed.Incr() == int64(majority) {
					onMajoritySubscribed()
				}
			})
			if err != nil {
				s.errorf(err, "sentinel-[%s] subscribe failed", sentinel)
			}
			results <- notified
		}(sentinels[i])
	}

	for alive := len(sentinels); ; alive-- {
		// 如果超过半数sentinel都没有订阅成功
		if alive < majority {
			if cntx.Err() == nil {
				s.printf("sentinel subscribe lost majority (%d/%d)", alive, len(sentinels))
			}
			return false
		}
		select {
		case <-cntx.Done():
			if cntx.Err() != context.DeadlineExceeded {
				s.printf("sentinel subscribe canceled (%v)", cntx.Err())
			}
			return false
		case notified := <-results:
			if notified {
				s.printf("sentinel subscribe notified +switch-master")
				return true
			}
		}
	}
}

func (s *Sentinel) existsCommand(client *Client, names []string) (map[string]bool, error) {
	defer func() {
		if !client.isRecyclable() {
			client.Close()
		}
	}()
	go func() {
		for _, name := range names {
			client.Send("SENTINEL", "get-master-addr-by-name", name)
		}
		if len(names) != 0 {
			client.Flush()
		}
	}()
	exists := make(map[string]bool, len(names))
	for _, name := range names {
		r, err := client.Receive()
		if err != nil {
			return nil, errors.Trace(err)
		}
		exists[name] = (r != nil)
	}
	return exists, nil
}

func (s *Sentinel) slavesCommand(client *Client, names []string) (map[string][]map[string]string, error) {
	defer func() {
		if !client.isRecyclable() {
			client.Close()
		}
	}()
	exists, err := s.existsCommand(client, names)
	if err != nil {
		return nil, err
	}
	go func() {
		var pending int
		for _, name := range names {
			if !exists[name] {
				continue
			}
			pending++
			client.Send("SENTINEL", "slaves", name)
		}
		if pending != 0 {
			client.Flush()
		}
	}()
	results := make(map[string][]map[string]string, len(names))
	for _, name := range names {
		if !exists[name] {
			continue
		}
		values, err := redigo.Values(client.Receive())
		if err != nil {
			return nil, errors.Trace(err)
		}
		var slaves []map[string]string
		for i := range values {
			m, err := redigo.StringMap(values[i], nil)
			if err != nil {
				return nil, errors.Trace(err)
			}
			slaves = append(slaves, m)
		}
		results[name] = slaves
	}
	return results, nil
}

func (s *Sentinel) mastersCommand(client *Client) (map[int]map[string]string, error) {
	defer func() {
		if !client.isRecyclable() {
			client.Close()
		}
	}()
	values, err := redigo.Values(client.Do("SENTINEL", "masters"))
	if err != nil {
		return nil, errors.Trace(err)
	}
	var masters = make(map[int]map[string]string)
	for i := range values {
		p, err := redigo.StringMap(values[i], nil)
		if err != nil {
			return nil, errors.Trace(err)
		}
		// 从值里面解析出gid
		gid, yes := s.isSameProduct(p["name"])
		if yes {
			masters[gid] = p
		}
	}
	return masters, nil
}

func (s *Sentinel) mastersDispatch(ctx context.Context, sentinel string, timeout time.Duration) (map[int]*SentinelMaster, error) {
	var masters = make(map[int]*SentinelMaster)
	var err = s.dispatch(ctx, sentinel, timeout, func(c *Client) error {
		p, err := s.mastersCommand(c)
		if err != nil {
			return err
		}
		for gid, master := range p {
			epoch, err := strconv.ParseInt(master["config-epoch"], 10, 64)
			if err != nil {
				s.printf("sentinel-[%s] masters parse %s failed, config-epoch = '%s', %s",
					sentinel, master["name"], master["config-epoch"], err)
				continue
			}
			var ip, port = master["ip"], master["port"]
			if ip == "" || port == "" {
				s.printf("sentinel-[%s] masters parse %s failed, ip:port = '%s:%s'",
					sentinel, master["name"], ip, port)
				continue
			}
			masters[gid] = &SentinelMaster{
				Addr: net.JoinHostPort(ip, port),
				Info: master, Epoch: epoch,
			}
		}
		return nil
	})
	if err != nil {
		switch errors.Cause(err) {
		case context.Canceled:
			return nil, nil
		default:
			return nil, err
		}
	}
	return masters, nil
}

type SentinelMaster struct {
	Addr  string
	Info  map[string]string
	Epoch int64
}

func (s *Sentinel) Masters(sentinels []string, timeout time.Duration) (map[int]string, error) {
	cntx, cancel := context.WithTimeout(s.Context, timeout)
	defer cancel()

	timeout += time.Second * 5
	results := make(chan map[int]*SentinelMaster, len(sentinels))

	var majority = 1 + len(sentinels)/2

	for i := range sentinels {
		go func(sentinel string) {
			// 通过sentinel info命令得到哨兵感知到的master
			masters, err := s.mastersDispatch(cntx, sentinel, timeout)
			if err != nil {
				s.errorf(err, "sentinel-[%s] masters failed", sentinel)
			}
			results <- masters
		}(sentinels[i])
	}

	masters := make(map[int]string)
	current := make(map[int]*SentinelMaster)

	var voted int
	for alive := len(sentinels); ; alive-- {
		if alive == 0 {
			switch {
			case cntx.Err() != context.DeadlineExceeded && cntx.Err() != nil:
				s.printf("sentinel masters canceled (%v)", cntx.Err())
				return nil, errors.Trace(cntx.Err())
			case voted != len(sentinels):
				s.printf("sentinel masters voted = (%d/%d) masters = %d (%v)", voted, len(sentinels), len(masters), cntx.Err())
			}
			// 最终通过的方案必须是半数以上sentinel同意的。
			if voted < majority {
				return nil, errors.Errorf("lost majority (%d/%d)", voted, len(sentinels))
			}
			return masters, nil
		}
		select {
		case <-cntx.Done():
			switch {
			case cntx.Err() != context.DeadlineExceeded:
				s.printf("sentinel masters canceled (%v)", cntx.Err())
				return nil, errors.Trace(cntx.Err())
			default:
				s.printf("sentinel masters voted = (%d/%d) masters = %d (%v)", voted, len(sentinels), len(masters), cntx.Err())
			}
			if voted < majority {
				return nil, errors.Errorf("lost majority (%d/%d)", voted, len(sentinels))
			}
			return masters, nil
		case m := <-results:
			if m == nil {
				continue
			}
			// 构造sentinels选举出的master
			for gid, master := range m {
				if current[gid] == nil || current[gid].Epoch < master.Epoch {
					current[gid] = master
					masters[gid] = master.Addr
				}
			}
			voted += 1
		}
	}
}

type MonitorConfig struct {
	Quorum          int
	ParallelSyncs   int
	DownAfter       time.Duration
	FailoverTimeout time.Duration

	NotificationScript   string
	ClientReconfigScript string
}

func (s *Sentinel) monitorGroupsCommand(client *Client, sentniel string, config *MonitorConfig, groups map[int]*net.TCPAddr) error {
	defer func() {
		if !client.isRecyclable() {
			client.Close()
		}
	}()
	var names []string
	for gid := range groups {
		names = append(names, s.NodeName(gid))
	}
	if err := s.removeCommand(client, names); err != nil {
		return err
	}
	go func() {
		for gid, tcpAddr := range groups {
			var ip, port = tcpAddr.IP.String(), tcpAddr.Port
			client.Send("SENTINEL", "monitor", s.NodeName(gid), ip, port, config.Quorum)
		}
		if len(groups) != 0 {
			client.Flush()
		}
	}()
	for range groups {
		_, err := client.Receive()
		if err != nil {
			return errors.Trace(err)
		}
	}
	go func() {
		for gid := range groups {
			var args = []interface{}{"set", s.NodeName(gid)}
			if config.ParallelSyncs != 0 {
				/*
					在发生failover主备切换时，这个选项指定了最多可以有多少个slave同时对新的master进行同步，这个数字越小，完成
					failover所需的时间就越长，但是如果这个数字越大，就意味着越多的slave因为replication而不可用。可以通过将这个值
					设为1来保证每次只有一个slave处于不能处理命令请求的状态。也可理解为一次性修改几个slave指向新的new master。
				*/
				args = append(args, "parallel-syncs", config.ParallelSyncs)
			}
			if config.DownAfter != 0 {
				/*
					sentinel会向master发送心跳PING来确认master是否存活，如果master在"一定时间范围"内不回应pong或者回复了一个
					错误消息，那么这个sentinel会主观地（单方面地）认为这个master已经不可用了（S_DOWN)。而这个down-after-millseconds
					就是用来指定这个"一定时间范围"的，单位是毫秒。
				*/
				args = append(args, "down-after-milliseconds", int(config.DownAfter/time.Millisecond))
			}
			if config.FailoverTimeout != 0 {
				/*
					如果sentinel A推荐sentinel B去执行failover，B会等待一段时间后，自行再次对同一个master执行failover，这个等待
					的时间是通过failover-timeout配置项去配置的。从这个规则可以看出，sentinel集群中的sentinel不会在同一时刻并发去
					failover同一个master，第一个进行failover的sentinel如果失败了，另外一个将会在一定时间内进行重新failover，以此类推。
				*/
				args = append(args, "failover-timeout", int(config.FailoverTimeout/time.Millisecond))
			}
			if s.Auth != "" {
				args = append(args, "auth-pass", s.Auth)
			}
			if config.NotificationScript != "" {
				/*
					在集群failover时会触发执行指定的脚本。脚本的执行结果若为1，即稍后重试（最大重试次数为10）；若为2，则执行结束。并且
					脚本最大执行时间为60秒，超时会被终止执行。使用方法如sentinel notification-script mymaster ./check.sh
				*/
				args = append(args, "notification-script", config.NotificationScript)
			}
			if config.ClientReconfigScript != "" {
				/*
					在重新配置new master, new slave 过程，可以触发的脚本。
				*/
				args = append(args, "client-reconfig-script", config.ClientReconfigScript)
			}
			client.Send("SENTINEL", args...)
		}
		if len(groups) != 0 {
			client.Flush()
		}
	}()
	for range groups {
		_, err := client.Receive()
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (s *Sentinel) monitorGroupsDispatch(ctx context.Context, sentinel string, timeout time.Duration,
	config *MonitorConfig, groups map[int]*net.TCPAddr) error {
	var err = s.dispatch(ctx, sentinel, timeout, func(c *Client) error {
		return s.monitorGroupsCommand(c, sentinel, config, groups)
	})
	if err != nil {
		switch errors.Cause(err) {
		case context.Canceled:
			return nil
		default:
			return err
		}
	}
	return nil
}

func (s *Sentinel) MonitorGroups(sentinels []string, timeout time.Duration, config *MonitorConfig, groups map[int]string) error {
	cntx, cancel := context.WithTimeout(s.Context, timeout)
	defer cancel()

	resolve := make(map[int]*net.TCPAddr)

	var exit = make(chan error, 1)

	go func() (err error) {
		defer func() {
			exit <- err
		}()
		for gid, addr := range groups {
			if err := cntx.Err(); err != nil {
				return errors.Trace(err)
			}
			tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
			if err != nil {
				s.printf("sentinel monitor resolve tcp address of %s failed, %s", addr, err)
				return errors.Trace(err)
			}
			resolve[gid] = tcpAddr
		}
		return nil
	}()

	select {
	case <-cntx.Done():
		if cntx.Err() != context.DeadlineExceeded {
			s.printf("sentinel monitor canceled (%v)", cntx.Err())
		} else {
			s.printf("sentinel montior resolve tcp address (%v)", cntx.Err())
		}
		return errors.Trace(cntx.Err())
	case err := <-exit:
		if err != nil {
			return err
		}
	}

	timeout += time.Second * 5
	results := make(chan error, len(sentinels))

	for i := range sentinels {
		go func(sentinel string) {
			// 调用SENTINEL MONITOR命令，监控集群中的group。监控之后，根据dashboard.toml中设置的sentinel参宿对sentinel进行设置
			err := s.monitorGroupsDispatch(cntx, sentinel, timeout, config, resolve)
			if err != nil {
				s.errorf(err, "sentinel-[%s] monitor failed", sentinel)
			}
			results <- err
		}(sentinels[i])
	}

	var last error
	for range sentinels {
		select {
		case <-cntx.Done():
			if last != nil {
				return last
			}
			return errors.Trace(cntx.Err())
		case err := <-results:
			if err != nil {
				last = err
			}
		}
	}
	return last
}

func (s *Sentinel) removeCommand(client *Client, names []string) error {
	defer func() {
		if !client.isRecyclable() {
			client.Close()
		}
	}()
	exists, err := s.existsCommand(client, names)
	if err != nil {
		return err
	}
	go func() {
		var pending int
		for _, name := range names {
			if !exists[name] {
				continue
			}
			pending++
			client.Send("SENTINEL", "remove", name)
		}
		if pending != 0 {
			client.Flush()
		}
	}()
	for _, name := range names {
		if !exists[name] {
			continue
		}
		_, err := client.Receive()
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (s *Sentinel) removeGroupsDispatch(ctx context.Context, sentinel string, timeout time.Duration,
	groups map[int]bool) error {
	var names []string
	for gid := range groups {
		names = append(names, s.NodeName(gid))
	}
	var err = s.dispatch(ctx, sentinel, timeout, func(c *Client) error {
		return s.removeCommand(c, names)
	})
	if err != nil {
		switch errors.Cause(err) {
		case context.Canceled:
			return nil
		default:
			return err
		}
	}
	return nil
}

func (s *Sentinel) RemoveGroups(sentinels []string, timeout time.Duration, groups map[int]bool) error {
	cntx, cancel := context.WithTimeout(s.Context, timeout)
	defer cancel()

	timeout += time.Second * 5
	results := make(chan error, len(sentinels))

	for i := range sentinels {
		go func(sentinel string) {
			err := s.removeGroupsDispatch(cntx, sentinel, timeout, groups)
			if err != nil {
				s.errorf(err, "sentinel-[%s] remove failed", sentinel)
			}
			results <- err
		}(sentinels[i])
	}

	var last error
	for range sentinels {
		select {
		case <-cntx.Done():
			if last != nil {
				return last
			}
			return errors.Trace(cntx.Err())
		case err := <-results:
			if err != nil {
				last = err
			}
		}
	}
	return last
}

func (s *Sentinel) removeGroupsAllDispatch(ctx context.Context, sentinel string, timeout time.Duration) error {
	var err = s.dispatch(ctx, sentinel, timeout, func(c *Client) error {
		masters, err := s.mastersCommand(c)
		if err != nil {
			return err
		}
		var names []string
		for gid := range masters {
			names = append(names, s.NodeName(gid))
		}
		return s.removeCommand(c, names)
	})
	if err != nil {
		switch errors.Cause(err) {
		case context.Canceled:
			return nil
		default:
			return err
		}
	}
	return nil
}

func (s *Sentinel) RemoveGroupsAll(sentinels []string, timeout time.Duration) error {
	cntx, cancel := context.WithTimeout(s.Context, timeout)
	defer cancel()

	timeout += time.Second * 5
	results := make(chan error, len(sentinels))

	for i := range sentinels {
		go func(sentinel string) {
			// 传入sentinel的地址，调用NewClient方法，新建一个sentinel的redisClient连接
			// 然后，调用SENTINEL masters显示被这个sentinel监控的所有master以及它们的状态
			// 如果这个sentinel目前对于productName-groupId这个group有监控
			// 就使用SENTINEL REMOTE <name>命令sentinel逐个放弃上面的监听。
			err := s.removeGroupsAllDispatch(cntx, sentinel, timeout)
			if err != nil {
				s.errorf(err, "sentinel-[%s] remove failed", sentinel)
			}
			results <- err
		}(sentinels[i])
	}

	var last error
	for range sentinels {
		select {
		// 当一个Context被显示cancel或者超时，Done会返回一个关闭的channel
		case <-cntx.Done():
			if last != nil {
				return last
			}
			// 当Done这个channel被关闭，Err说明了Context被cancel的原因
			return errors.Trace(cntx.Err())
		case err := <-results:
			if err != nil {
				last = err
			}
		}
	}
	return last
}

type SentinelGroup struct {
	Master map[string]string   `json:"master"`
	Slaves []map[string]string `json:"slaves,omitempty"`
}

func (s *Sentinel) MastersAndSlavesClient(client *Client) (map[string]*SentinelGroup, error) {
	defer func() {
		if !client.isRecyclable() {
			client.Close()
		}
	}()
	// 发送SENTINEL masters命令：列出所有被监视的主服务器，以及这些主服务器的当前状态
	masters, err := s.mastersCommand(client)
	if err != nil {
		return nil, err
	}
	var names []string
	for gid := range masters {
		names = append(names, s.NodeName(gid))
	}
	// 发送SENTINEL slaves <master-name>命令：列出给定主服务器的所有从服务器，以及这些从服务器的当前状态
	slaves, err := s.slavesCommand(client, names)
	if err != nil {
		return nil, err
	}
	results := make(map[string]*SentinelGroup, len(masters))
	for gid, master := range masters {
		var name = s.NodeName(gid)
		results[name] = &SentinelGroup{
			Master: master, Slaves: slaves[name],
		}
	}
	return results, nil
}

func (s *Sentinel) MastersAndSlaves(sentinel string, timeout time.Duration) (map[string]*SentinelGroup, error) {
	var results map[string]*SentinelGroup
	var err = s.do(sentinel, timeout, func(c *Client) error {
		m, err := s.MastersAndSlavesClient(c)
		if err != nil {
			return err
		}
		results = m
		return nil
	})
	if err != nil {
		return nil, err
	}
	return results, nil
}

func (s *Sentinel) FlushConfig(sentinel string, timeout time.Duration) error {
	return s.do(sentinel, timeout, func(c *Client) error {
		_, err := c.Do("SENTINEL", "flushconfig")
		if err != nil {
			return err
		}
		return nil
	})
}
