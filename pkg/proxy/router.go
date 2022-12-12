// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package proxy

import (
	"net"
	"reflect"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/recuffer/pkg/models"
	"github.com/recuffer/pkg/proxy/internal/hashtag"
	"github.com/recuffer/pkg/utils"
	"github.com/recuffer/pkg/utils/errors"
	"github.com/recuffer/pkg/utils/log"
	"github.com/recuffer/pkg/utils/redis"
)

const MaxSlotNum = models.MaxSlotNum

type Router struct {
	mu sync.RWMutex

	pool struct {
		primary *sharedBackendConnPool
		replica *sharedBackendConnPool
	}
	slots [MaxSlotNum]Slot

	config           *Config
	online           bool
	closed           bool
	nodes            map[string]string         // 存放集群所有节点的切片
	movedpool        map[string]*MovedConnPool // 存放处理moved、ask请求的池子
	readblacklist    []string                  // 存放读黑名单的切片
	clientipblackmap map[string]bool           // 存放client ip黑名单的map
	cmdblackmap      map[string][]string       // 存放CMD黑名单的map,key为key,value为cmd list
}

func NewRouter(config *Config) *Router {
	s := &Router{config: config}
	s.pool.primary = newSharedBackendConnPool(config, config.BackendPrimaryParallel)
	s.pool.replica = newSharedBackendConnPool(config, config.BackendReplicaParallel)
	s.movedpool = make(map[string]*MovedConnPool)
	// 根据配置文件初始化client ip黑名单
	s.clientipblackmap = make(map[string]bool)
	clientIpBlackList := strings.Split(config.ClientIpBlackList, ",")
	if len(clientIpBlackList) > 0 {
		for _, v := range clientIpBlackList {
			if net.ParseIP(v) != nil {
				s.setClientIpBlackList(v)
			} else {
				log.Warnf("router SetClientIpBlackList fail ,the ip format is not correct")
			}
		}
	}
	// 根据配置文件初始化cmd黑名单
	s.cmdblackmap = make(map[string][]string)
	cmdBlackList := strings.Split(config.CMDBlackList, ",")
	if len(cmdBlackList) > 0 {
		for _, v := range cmdBlackList {
			cmd_key_list := strings.Split(v, " ")
			if len(cmd_key_list) != 2 {
				log.Warnf("router Set cmdblackmap fail ,the cmd format is not correct")
				continue
			}
			cmd := cmd_key_list[0]
			cmd = strings.ToUpper(string(cmd))
			key := cmd_key_list[1]
			s.setCMDBlackList(key, cmd)
		}
	}
	// 一开始应该没有槽位
	for i := range s.slots {
		s.slots[i].id = i
		s.slots[i].method = &forwardSync{}
	}
	return s
}

func (s *Router) Start() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.online = true
}

func (s *Router) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true

	for i := range s.slots {
		s.fillSlot(&models.Slot{Id: i}, false, nil)
	}
}

func (s *Router) GetSlots() []*models.Slot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	slots := make([]*models.Slot, MaxSlotNum)
	for i := range s.slots {
		slots[i] = s.slots[i].snapshot()
	}
	return slots
}

func (s *Router) GetSlot(id int) *models.Slot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if id < 0 || id >= MaxSlotNum {
		return nil
	}
	slot := &s.slots[id]
	return slot.snapshot()
}

func (s *Router) HasSwitched() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for i := range s.slots {
		if s.slots[i].switched {
			return true
		}
	}
	return false
}

var (
	ErrClosedRouter  = errors.New("use of closed router")
	ErrInvalidSlotId = errors.New("use of invalid slot id")
	ErrInvalidMethod = errors.New("use of invalid forwarder method")
)

func (s *Router) FillSlot(m *models.Slot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosedRouter
	}
	if m.Id < 0 || m.Id >= MaxSlotNum {
		return ErrInvalidSlotId
	}
	var method forwardMethod
	switch m.ForwardMethod {
	default:
		return ErrInvalidMethod
	case models.ForwardSync:
		method = &forwardSync{}
	case models.ForwardSemiAsync:
		method = &forwardSemiAsync{}
	}
	s.fillSlot(m, false, method)
	return nil
}

func (s *Router) KeepAlive() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return ErrClosedRouter
	}
	s.pool.primary.KeepAlive()
	s.pool.replica.KeepAlive()
	return nil
}

func (s *Router) isOnline() bool {
	return s.online && !s.closed
}

func BytesToStr(b []byte) string {
	bh := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	sh := reflect.StringHeader{Data: bh.Data, Len: bh.Len}
	return *(*string)(unsafe.Pointer(&sh))
}

// 根据key进行转发，这里将原先的获取key的slot方式，修改为通过key名字，计算rediscluster的slot
func (s *Router) dispatch(r *Request) error {
	hkey := getHashKey(r.Multi, r.OpStr)
	key_name := BytesToStr(hkey)
	if len(s.cmdblackmap) != 0 {
		r_err := errors.New("command was rejected because it was in the blacklist")
		if s.findCMDBlackList(key_name, r.OpStr) {
			return r_err
		}
	}
	// var id = Hash(hkey) % MaxSlotNum
	var id = hashtag.Slot(key_name)
	slot := &s.slots[id]
	// 将请求分发到相应的slot
	return slot.forward(r, hkey)
}

// 将request发到指定slot
func (s *Router) dispatchSlot(r *Request, id int) error {
	if id < 0 || id >= MaxSlotNum {
		return ErrInvalidSlotId
	}
	slot := &s.slots[id]
	// 将请求分发到相应的slot
	return slot.forward(r, nil)
}

// 判断屏蔽节点是否已经在黑名单里
func (s *Router) findReadBlackList(addr string) bool {
	for _, v := range s.readblacklist {
		if addr == v {
			return true
		}
	}
	return false
}

func (s *Router) setReadBlackList(addr string) {
	isHasAddr := s.findReadBlackList(addr)
	if !isHasAddr {
		s.readblacklist = append(s.readblacklist, addr)
	}
}

func (s *Router) delReadBlackList(addr string) {
	s.readblacklist = utils.SliceDelStr(s.readblacklist, addr)
}

func (s *Router) setClientIpBlackList(addr string) {
	if _, ok := s.clientipblackmap[addr]; !ok {
		s.clientipblackmap[addr] = true
	}
}

func (s *Router) delClientIpBlackList(addr string) {
	if _, ok := s.clientipblackmap[addr]; ok {
		delete(s.clientipblackmap, addr)
	}
}

// 判断cmd key是否已经在黑名单里
func (s *Router) findCMDBlackList(key string, cmd string) bool {
	if _, ok := s.cmdblackmap[key]; ok {
		for _, v := range s.cmdblackmap[key] {
			if cmd == v {
				return true
			}
		}
	}
	return false
}

func (s *Router) setCMDBlackList(key string, cmd string) {
	isHasCMD := s.findCMDBlackList(key, cmd)
	if !isHasCMD {
		s.cmdblackmap[key] = append(s.cmdblackmap[key], cmd)
	}
}

func (s *Router) delCMDBlackList(key string, cmd string) {
	isHasCMD := s.findCMDBlackList(key, cmd)
	if isHasCMD {
		s.cmdblackmap[key] = utils.SliceDelStr(s.cmdblackmap[key], cmd)
		if len(s.cmdblackmap[key]) == 0 {
			delete(s.cmdblackmap, key)
		}
	}
}

// 判断节点是否已经在nodes
func (s *Router) findNodes(addr string) bool {
	if s.nodes != nil {
		if _, ok := s.nodes[addr]; ok {
			return true
		} else {
			return false
		}
	} else {
		return false
	}
}

// 设置集群所有节点
func (s *Router) setNodes(addr string) {
	if s.nodes == nil {
		s.nodes = make(map[string]string)
	}
	isHasAddr := s.findNodes(addr)
	if !isHasAddr {
		s.nodes[addr] = addr
	}
}

// 删除已经不存在的节点
func (s *Router) delNodes(allNodes map[string]string) {
	if s.nodes != nil && allNodes != nil {
		for k := range s.nodes {
			if _, ok := allNodes[k]; !ok {
				delete(s.nodes, k)
			}
		}
	}
}

// 处理掉moved pool已经下线地址对应的连接池
func (s *Router) delMovedPool() {
	if s.movedpool != nil && s.nodes != nil {
		for k := range s.movedpool {
			if _, ok := s.nodes[k]; !ok {
				log.Warnf("moved pool clear offline addr :%v pool.", k)
				err := s.movedpool[k].Close()
				if err != nil {
					log.Warnf("moved pool clear offline addr pool fail, err: %v", err)
				}
				delete(s.movedpool, k)
			}
		}
	}
}

// 这个是指明了redis服务器地址的请求，如果找不到就返回false
func (s *Router) dispatchAddr(r *Request, addr string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if bc := s.pool.primary.Get(addr).BackendConn(r.Database, r.Seed16(), false); bc != nil {
		bc.PushBack(r)
		return true
	}
	if bc := s.pool.replica.Get(addr).BackendConn(r.Database, r.Seed16(), false); bc != nil {
		bc.PushBack(r)
		return true
	}
	return false
}

// proxy/router中存储了集群中所有sharedBackendConnPool和slot，用于将redis请求转发给相应的slot进行处理，而
// router里面的sharedBackendConnPool和slot就是在这里进行填充的。
// 每个slot都被分配了相应的backendConn，只不过此时每个backendConn都为空
func (s *Router) fillSlot(m *models.Slot, switched bool, method forwardMethod) {
	slot := &s.slots[m.Id]
	// 将slot的lock.hold属性设为true，并加锁。
	slot.blockAndWait()

	// 清空models.Slot里面的backendConn
	// 这里的bc就是我们之前提到过的处理redis请求的sharedBackendConn。第一次进来的时候，backend为空，这里直接返回。
	slot.backend.bc.Release()
	slot.backend.bc = nil
	slot.backend.id = 0
	slot.migrate.bc.Release()
	slot.migrate.bc = nil
	slot.migrate.id = 0
	for i := range slot.replicaGroups {
		for _, bc := range slot.replicaGroups[i] {
			bc.Release()
		}
	}
	slot.replicaGroups = nil

	slot.switched = switched

	// 初始阶段addr和from都是空字符串
	if addr := m.BackendAddr; len(addr) != 0 {
		// 从Router的primary sharedBackendConnPool中取出addr对应的sharedBackendConn，如果没有就新建并且放入，也相当于初始化了。
		slot.backend.bc = s.pool.primary.Retain(addr, s)
		slot.backend.id = m.BackendAddrGroupId
	}
	if from := m.MigrateFrom; len(from) != 0 {
		slot.migrate.bc = s.pool.primary.Retain(from, s)
		slot.migrate.id = m.MigrateFromGroupId
	}
	if !s.config.BackendPrimaryOnly {
		for i := range m.ReplicaGroups {
			var group []*sharedBackendConn
			for _, addr := range m.ReplicaGroups[i] {
				group = append(group, s.pool.replica.Retain(addr, s))
			}
			if len(group) == 0 {
				continue
			}
			slot.replicaGroups = append(slot.replicaGroups, group)
		}
	}
	if method != nil {
		slot.method = method
	}
	if !m.Locked {
		slot.unblock()
	}
	if !s.closed {
		if slot.migrate.bc != nil {
			if switched {
				log.Warnf("fill slot %04d, backend.addr = %s, migrate.from = %s, locked = %t, +switched",
					slot.id, slot.backend.bc.Addr(), slot.migrate.bc.Addr(), slot.lock.hold)
			} else {
				log.Warnf("fill slot %04d, backend.addr = %s, migrate.from = %s, locked = %t",
					slot.id, slot.backend.bc.Addr(), slot.migrate.bc.Addr(), slot.lock.hold)
			}
		} else {
			if switched {
				log.Warnf("fill slot %04d, backend.addr = %s, locked = %t, +switched",
					slot.id, slot.backend.bc.Addr(), slot.lock.hold)
			} else {
				log.Warnf("fill slot %04d, backend.addr = %s, locked = %t",
					slot.id, slot.backend.bc.Addr(), slot.lock.hold)
			}
		}
	}
}

func (s *Router) SwitchMasters(masters map[int]string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosedRouter
	}
	cache := &redis.InfoCache{
		Auth: s.config.ProductAuth, Timeout: time.Millisecond * 100,
	}
	for i := range s.slots {
		s.trySwitchMaster(i, masters, cache)
	}
	return nil
}

func (s *Router) trySwitchMaster(id int, masters map[int]string, cache *redis.InfoCache) {
	var switched bool
	var m = s.slots[id].snapshot()

	hasSameRunId := func(addr1, addr2 string) bool {
		if addr1 != addr2 {
			rid1 := cache.GetRunId(addr1)
			rid2 := cache.GetRunId(addr2)
			return rid1 != "" && rid1 == rid2
		}
		return true
	}

	if addr := masters[m.BackendAddrGroupId]; addr != "" {
		if !hasSameRunId(addr, m.BackendAddr) {
			m.BackendAddr, switched = addr, true
		}
	}
	if addr := masters[m.MigrateFromGroupId]; addr != "" {
		if !hasSameRunId(addr, m.MigrateFrom) {
			m.MigrateFrom, switched = addr, true
		}
	}
	if switched {
		s.fillSlot(m, true, nil)
	}
}
