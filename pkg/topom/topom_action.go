// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package topom

import (
	"fmt"
	"strconv"
	"time"

	"github.com/recuffer/pkg/models"
	"github.com/recuffer/pkg/utils/log"
	"github.com/recuffer/pkg/utils/math2"
	"github.com/recuffer/pkg/utils/sync2"
)

func (s *Topom) ProcessSlotAction() error {
	for s.IsOnline() {
		var (
			marks = make(map[int]bool)
			// 分配slot的时候点击弹窗的confirm只会，这个plans才能取出值
			plans = make(map[int]bool)
		)
		// 如果marks存在该groupId或m.Action.TargetId，返回false
		// 如果m被update函数处理过，那么accept(m)会返回true
		var accept = func(m *models.SlotMapping) bool {
			if marks[m.GroupId] || marks[m.Action.TargetId] {
				return false
			}
			if plans[m.Id] {
				return false
			}
			return true
		}
		// update函数对plans和marks进行初始化
		var update = func(m *models.SlotMapping) bool {
			// 只有在槽当前的GroupId为0的时候，marks[m.GroupId]才是false
			if m.GroupId != 0 {
				marks[m.GroupId] = true
			}
			marks[m.Action.TargetId] = true
			plans[m.Id] = true
			return true
		}

		// 按照默认的配置文件，这个值是100，并行迁移的slot数量，是一个阈值
		var parallel = math2.MaxInt(1, s.config.MigrationParallelSlots)
		// 一开始plans为空，所以下面的方法一定会执行一次，这个过程plans会初始化。后面如果plans的长度大于100，就直接对所有plans做处理；
		// 否则如果集群中所有Slotmapping中Action.state最小的那个Slotmapping如果处于pending,preparing或者prepared，也可以跳出循环对plans进行处理
		for parallel > len(plans) {
			// 对是否满足plans的处理情况做过滤。
			_, ok, err := s.SlotActionPrepareFilter(accept, update)
			if err != nil {
				return err
			} else if !ok {
				break
			}
		}
		// 在指定slot的分配plan之前，这个一直返回nil
		if len(plans) == 0 {
			return nil
		}
		var fut sync2.Future
		// 遍历plans，从中取出具体的每个slot的迁移计划，其中plans的key是每一个slot的id，value是要迁移到的groupId
		for sid, _ := range plans {
			fut.Add()
			go func(sid int) {
				log.Warnf("slot-[%d] process action", sid)
				// 针对每个slot做处理
				var err = s.processSlotAction(sid)
				if err != nil {
					status := fmt.Sprintf("[ERROR] Slot[%04d]: %s", sid, err)
					s.action.progress.status.Store(status)
				} else {
					s.action.progress.status.Store("")
				}
				// Future结构体中的vmap中存储了slotid和对应的error之间的映射关系
				fut.Done(strconv.Itoa(sid), err)
			}(sid)
		}

		// 当所有slot操作结束之后，遍历Future的vmap，取出有error的并返回
		for _, v := range fut.Wait() {
			if v != nil {
				return v.(error)
			}
		}
		time.Sleep(time.Millisecond * 10)
	}
	return nil
}

func (s *Topom) processSlotAction(sid int) error {
	var db int = 0
	for s.IsOnline() {
		// 返回的exec就是具体的slot操作执行函数
		if exec, err := s.newSlotActionExecutor(sid); err != nil {
			return err
		} else if exec == nil {
			time.Sleep(time.Second)
		} else {
			n, nextdb, err := exec(db)
			if err != nil {
				return err
			}
			log.Debugf("slot-[%d] action executor %d", sid, n)

			if n == 0 && nextdb == -1 {
				return s.SlotActionComplete(sid)
			}
			status := fmt.Sprintf("[OK] Slot[%04d]@DB[%d]=%d", sid, db, n)
			s.action.progress.status.Store(status)

			if us := s.GetSlotActionInterval(); us != 0 {
				time.Sleep(time.Microsecond * time.Duration(us))
			}
			db = nextdb
		}
	}
	return nil
}

// 处理同步操作，指的是一个group中的主从codis-server服务器之间进行数据的同步，GroupServer是Group的一个属性，标明了当前
// group中的所有codis-server的地址和action。
func (s *Topom) ProcessSyncAction() error {
	// 执行同步操作之前做的准备工作
	addr, err := s.SyncActionPrepare()
	if err != nil || addr == "" {
		return err
	}
	log.Warnf("sync-[%s] process action", addr)

	// 执行同步操作
	exec, err := s.newSyncActionExecutor(addr)
	if err != nil || exec == nil {
		return err
	}
	return s.SyncActionComplete(addr, exec() != nil)
}
