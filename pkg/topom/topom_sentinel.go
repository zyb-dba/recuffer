// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package topom

import (
	"time"

	"github.com/recuffer/pkg/models"
	"github.com/recuffer/pkg/utils/errors"
	"github.com/recuffer/pkg/utils/log"
	"github.com/recuffer/pkg/utils/math2"
	"github.com/recuffer/pkg/utils/redis"
	"github.com/recuffer/pkg/utils/sync2"
)

func (s *Topom) AddSentinel(addr string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	if addr == "" {
		return errors.Errorf("invalid sentinel address")
	}
	p := ctx.sentinel

	for _, x := range p.Servers {
		if x == addr {
			return errors.Errorf("sentinel-[%s] already exists", addr)
		}
	}

	sentinel := redis.NewSentinel(s.config.ProductName, s.config.ProductAuth)
	if err := sentinel.FlushConfig(addr, s.config.SentinelClientTimeout.Duration()); err != nil {
		return err
	}
	defer s.dirtySentinelCache()

	p.Servers = append(p.Servers, addr)
	p.OutOfSync = true
	return s.storeUpdateSentinel(p)
}

func (s *Topom) DelSentinel(addr string, force bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	if addr == "" {
		return errors.Errorf("invalid sentinel address")
	}
	p := ctx.sentinel

	var slice []string
	for _, x := range p.Servers {
		if x != addr {
			slice = append(slice, x)
		}
	}
	if len(slice) == len(p.Servers) {
		return errors.Errorf("sentinel-[%s] not found", addr)
	}
	defer s.dirtySentinelCache()

	p.OutOfSync = true
	if err := s.storeUpdateSentinel(p); err != nil {
		return err
	}

	sentinel := redis.NewSentinel(s.config.ProductName, s.config.ProductAuth)
	if err := sentinel.RemoveGroupsAll([]string{addr}, s.config.SentinelClientTimeout.Duration()); err != nil {
		log.WarnErrorf(err, "remove sentinel %s failed", addr)
		if !force {
			return errors.Errorf("remove sentinel %s failed", addr)
		}
	}

	p.Servers = slice
	return s.storeUpdateSentinel(p)
}

func (s *Topom) SwitchMasters(masters map[int]string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosedTopom
	}
	s.ha.masters = masters

	if len(masters) != 0 {
		cache := &redis.InfoCache{
			Auth: s.config.ProductAuth, Timeout: time.Millisecond * 100,
		}
		for gid, master := range masters {
			// 执行codis集群可感知的主从切换
			if err := s.trySwitchGroupMaster(gid, master, cache); err != nil {
				log.WarnErrorf(err, "sentinel switch group master failed")
			}
		}
	}
	return nil
}

func (s *Topom) rewatchSentinels(servers []string) {
	if s.ha.monitor != nil {
		s.ha.monitor.Cancel()
		s.ha.monitor = nil
	}
	if len(servers) == 0 {
		s.ha.masters = nil
	} else {
		// 创建Topom中的ha.monitor
		s.ha.monitor = redis.NewSentinel(s.config.ProductName, s.config.ProductAuth)
		s.ha.monitor.LogFunc = log.Warnf
		s.ha.monitor.ErrFunc = log.WarnErrorf
		go func(p *redis.Sentinel) {
			var trigger = make(chan struct{}, 1)
			// 一个延时工具类，要么休眠一秒，要么休眠现在距离deadline的时间，取决于哪个更短
			// 如果现在已经过了deadline，就不休眠
			delayUntil := func(deadline time.Time) {
				// 如果从Sentinel中Context.Done()读出值，就表示这个sentinel的context已经被cancel
				for !p.IsCanceled() {
					var d = deadline.Sub(time.Now())
					if d <= 0 {
						return
					}
					time.Sleep(math2.MinDuration(d, time.Second))
				}
			}
			go func() {
				defer close(trigger)
				callback := func() {
					select {
					case trigger <- struct{}{}:
					default:
					}
				}
				for !p.IsCanceled() {
					timeout := time.Minute * 15
					retryAt := time.Now().Add(time.Second * 10)
					/*
						到这里，是集群中的sentinel订阅了redis服务器之间主从切换的信息，只有哨兵知道哪台是master。对于codis集群
						来讲，并不清楚哪台slave被推上了master。下面我们要做的，就是让哨兵感知到的新的master同样被codis集群感知到，
						也就是将其推到每个group的第一台server。
					*/
					if !p.Subscribe(servers, timeout, callback) {
						delayUntil(retryAt)
					} else {
						callback()
					}
				}
			}()
			go func() {
				for range trigger {
					var success int
					for i := 0; i != 10 && !p.IsCanceled() && success != 2; i++ {
						timeout := time.Second * 5
						/*
							最后一步，通过setinel info命令得到当前的主服务器，然后在各个group中更新主服务器信息。比方说，
							如果超过半数sentinel认为group中序号为1的server才是master，就把这台服务器和序号为0的server进行交换。
						*/
						masters, err := p.Masters(servers, timeout)
						if err != nil {
							log.WarnErrorf(err, "fetch group masters failed")
						} else {
							if !p.IsCanceled() {
								s.SwitchMasters(masters)
							}
							success += 1
						}
						delayUntil(time.Now().Add(time.Second * 5))
					}
				}
			}()
		}(s.ha.monitor)
	}
	log.Warnf("rewatch sentinels = %v", servers)
}

/*
总结：当一台sentinel第一次被添加到codis集群，或者是脱离codis集群之后，需要执行resync操作来重新对集群做监控。首先遍历所有server，
放弃其原先监控的信息。格式化之后，再重新监控集群中的所有group,并根据dashboard.toml中的配置进行监控设置。最后，新建Topom.ha.monitor
上帝视角sentinel，让集群中的所有sentinel订阅"+switch-master"，如果发生主从切换（既可以从channel中读出值），要从哨兵中读出当前的
master地址，并在每个codis group中将对应的server推到group的第一个。设置每个Proxy的ha.servers为当前ctx中的sentinel，再执行一次
上面的rewatchSentinels方法，最后再将sentinel的OutofSync更新为true，然后再更新zk下存储的信息。
*/
func (s *Topom) ResyncSentinels() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}
	defer s.dirtySentinelCache()

	p := ctx.sentinel
	p.OutOfSync = true
	if err := s.storeUpdateSentinel(p); err != nil {
		return err
	}

	// dashboard.toml中的配置项
	config := &redis.MonitorConfig{
		Quorum:               s.config.SentinelQuorum,
		ParallelSyncs:        s.config.SentinelParallelSyncs,
		DownAfter:            s.config.SentinelDownAfter.Duration(),
		FailoverTimeout:      s.config.SentinelFailoverTimeout.Duration(),
		NotificationScript:   s.config.SentinelNotificationScript,
		ClientReconfigScript: s.config.SentinelClientReconfigScript,
	}

	// 新建哨兵对象
	sentinel := redis.NewSentinel(s.config.ProductName, s.config.ProductAuth)
	// 这一步先让每个sentinel放弃之前的监听，相当于格式化。
	if err := sentinel.RemoveGroupsAll(p.Servers, s.config.SentinelClientTimeout.Duration()); err != nil {
		log.WarnErrorf(err, "remove sentinels failed")
	}
	// 在sentinel格式化之后，重新监控集群中的group.
	if err := sentinel.MonitorGroups(p.Servers, s.config.SentinelClientTimeout.Duration(), config, ctx.getGroupMasters()); err != nil {
		log.WarnErrorf(err, "resync sentinels failed")
		return err
	}
	s.rewatchSentinels(p.Servers)

	var fut sync2.Future
	for _, p := range ctx.proxy {
		fut.Add()
		go func(p *models.Proxy) {
			// s.newProxyClient(p)就是调用proxy.NewApiClient(p.AdminAddr)，得到一个ApiClient
			// 下一步调用s.proxy.SetSentinels(sentinel.Servers)

			// 在每个Proxy中设置其ha.servers为当前ctx中的sentinel，再执行一次上面的rewatchSentinels方法。
			err := s.newProxyClient(p).SetSentinels(ctx.sentinel)
			if err != nil {
				log.ErrorErrorf(err, "proxy-[%s] resync sentinel failed", p.Token)
			}
			fut.Done(p.Token, err)
		}(p)
	}
	for t, v := range fut.Wait() {
		switch err := v.(type) {
		case error:
			if err != nil {
				return errors.Errorf("proxy-[%s] sentinel failed", t)
			}
		}
	}

	p.OutOfSync = false

	return s.storeUpdateSentinel(p)
}
