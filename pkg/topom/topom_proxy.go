// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package topom

import (
	"github.com/recuffer/pkg/models"
	"github.com/recuffer/pkg/proxy"
	"github.com/recuffer/pkg/utils/errors"
	"github.com/recuffer/pkg/utils/log"
	"github.com/recuffer/pkg/utils/sync2"
)

// 由fe(topom_api.go)所调用
// 传入的addr就是proxy的地址
func (s *Topom) CreateProxy(addr string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	// 这里的p就是根据proxy地址addr取出的models.proxy信息
	p, err := proxy.NewApiClient(addr).Model()
	if err != nil {
		return errors.Errorf("proxy@%s fetch model failed, %s", addr, err)
	}
	// 这个ApiClient中存储了proxy的地址，以及根据productName,productAuth(默认为空)以及token生成的auth
	c := s.newProxyClient(p)

	// proxy启动的时候，会在s.setup(config)这一步，会生成一个xauth，存储在Proxy的xauth属性中，
	// 这一步就是讲上面得到的xauth和启动proxy时的xauth作比较，来唯一确定需要的xauth
	if err := c.XPing(); err != nil {
		return errors.Errorf("proxy@%s check xauth failed, %s", addr, err)
	}
	// 检查上下文中的proxy是否已经有token，如果有的话，说明这个proxy已经添加到集群中
	if ctx.proxy[p.Token] != nil {
		return errors.Errorf("proxy-[%s] already exists", p.Token)
	} else {
		// 上下文所有的proxy的最多id+1，以此作为当前proxy的id
		p.Id = ctx.maxProxyId() + 1
	}
	defer s.dirtyProxyCache(p.Token)

	// 到这一步，proxy已经添加成功，更新zk信息
	if err := s.storeCreateProxy(p); err != nil {
		return err
	} else {
		return s.reinitProxy(ctx, p, c)
	}
}

func (s *Topom) OnlineProxy(addr string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	p, err := proxy.NewApiClient(addr).Model()
	if err != nil {
		return errors.Errorf("proxy@%s fetch model failed", addr)
	}
	c := s.newProxyClient(p)

	if err := c.XPing(); err != nil {
		return errors.Errorf("proxy@%s check xauth failed", addr)
	}
	defer s.dirtyProxyCache(p.Token)

	if d := ctx.proxy[p.Token]; d != nil {
		p.Id = d.Id
		if err := s.storeUpdateProxy(p); err != nil {
			return err
		}
	} else {
		p.Id = ctx.maxProxyId() + 1
		if err := s.storeCreateProxy(p); err != nil {
			return err
		}
	}
	return s.reinitProxy(ctx, p, c)
}

func (s *Topom) RemoveProxy(token string, force bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	p, err := ctx.getProxy(token)
	if err != nil {
		return err
	}
	c := s.newProxyClient(p)

	if err := c.Shutdown(); err != nil {
		log.WarnErrorf(err, "proxy-[%s] shutdown failed, force remove = %t", token, force)
		if !force {
			return errors.Errorf("proxy-[%s] shutdown failed", p.Token)
		}
	}
	defer s.dirtyProxyCache(p.Token)

	return s.storeRemoveProxy(p)
}

func (s *Topom) ReinitProxy(token string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	p, err := ctx.getProxy(token)
	if err != nil {
		return err
	}
	c := s.newProxyClient(p)

	return s.reinitProxy(ctx, p, c)
}

func (s *Topom) newProxyClient(p *models.Proxy) *proxy.ApiClient {
	c := proxy.NewApiClient(p.AdminAddr)
	c.SetXAuth(s.config.ProductName, s.config.ProductAuth, p.Token)
	return c
}

func (s *Topom) reinitProxy(ctx *context, p *models.Proxy, c *proxy.ApiClient) error {
	log.Warnf("proxy-[%s] reinit:\n%s", p.Token, p.Encode())
	// 初始化1024个槽
	// 请求proxy的FillSlots方法，对每个slot的sharedBackendConnPool创建BackendConn
	if err := c.FillSlots(ctx.toSlotSlice(ctx.slots, p)...); err != nil {
		log.ErrorErrorf(err, "proxy-[%s] fillslots failed", p.Token)
		return errors.Errorf("proxy-[%s] fillslots failed", p.Token)
	}
	// 这里调用的是proxy暴露的start的cgi方法。
	if err := c.Start(); err != nil {
		log.ErrorErrorf(err, "proxy-[%s] start failed", p.Token)
		return errors.Errorf("proxy-[%s] start failed", p.Token)
	}
	// 由于此时sentinels还没有，传入的server队列为空，这个方法暂时不管
	if err := c.SetSentinels(ctx.sentinel); err != nil {
		log.ErrorErrorf(err, "proxy-[%s] set sentinels failed", p.Token)
		return errors.Errorf("proxy-[%s] set sentinels failed", p.Token)
	}
	return nil
}

func (s *Topom) resyncSlotMappingsByGroupId(ctx *context, gid int) error {
	return s.resyncSlotMappings(ctx, ctx.getSlotMappingsByGroupId(gid)...)
}

// 当一个SlotMapping处于preparing和prepared状态的时候，会将其状态推进到下一阶段，并同步SlotMapping,根据
// []*models.SlotMapping创建1024个models.Slot，再填充1024个pkg/proxy/slots.go中的Slot，此过程中Router为每个Slot
// 都分配了对应的backendConn。
func (s *Topom) resyncSlotMappings(ctx *context, slots ...*models.SlotMapping) error {
	if len(slots) == 0 {
		return nil
	}
	var fut sync2.Future
	// 遍历所有的proxy
	for _, p := range ctx.proxy {
		fut.Add()
		go func(p *models.Proxy) {
			// 调用proxy的FillSlots方法
			err := s.newProxyClient(p).FillSlots(ctx.toSlotSlice(slots, p)...)
			if err != nil {
				log.ErrorErrorf(err, "proxy-[%s] resync slots failed", p.Token)
			}
			fut.Done(p.Token, err)
		}(p)
	}
	for t, v := range fut.Wait() {
		switch err := v.(type) {
		case error:
			if err != nil {
				return errors.Errorf("proxy-[%s] resync slots failed", t)
			}
		}
	}
	return nil
}
