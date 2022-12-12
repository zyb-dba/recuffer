// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package topom

import (
	"sort"
	"time"

	rbtree "github.com/emirpasic/gods/trees/redblacktree"

	"github.com/recuffer/pkg/models"
	"github.com/recuffer/pkg/utils/errors"
	"github.com/recuffer/pkg/utils/log"
	"github.com/recuffer/pkg/utils/math2"
	"github.com/recuffer/pkg/utils/redis"
)

func (s *Topom) SlotCreateAction(sid int, gid int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	g, err := ctx.getGroup(gid)
	if err != nil {
		return err
	}
	if len(g.Servers) == 0 {
		return errors.Errorf("group-[%d] is empty", gid)
	}

	m, err := ctx.getSlotMapping(sid)
	if err != nil {
		return err
	}
	if m.Action.State != models.ActionNothing {
		return errors.Errorf("slot-[%d] action already exists", sid)
	}
	if m.GroupId == gid {
		return errors.Errorf("slot-[%d] already in group-[%d]", sid, gid)
	}
	defer s.dirtySlotsCache(m.Id)

	m.Action.State = models.ActionPending
	m.Action.Index = ctx.maxSlotActionIndex() + 1
	m.Action.TargetId = g.Id
	return s.storeUpdateSlotMapping(m)
}

func (s *Topom) SlotCreateActionSome(groupFrom, groupTo int, numSlots int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	g, err := ctx.getGroup(groupTo)
	if err != nil {
		return err
	}
	if len(g.Servers) == 0 {
		return errors.Errorf("group-[%d] is empty", g.Id)
	}

	var pending []int
	for _, m := range ctx.slots {
		if len(pending) >= numSlots {
			break
		}
		if m.Action.State != models.ActionNothing {
			continue
		}
		if m.GroupId != groupFrom {
			continue
		}
		if m.GroupId == g.Id {
			continue
		}
		pending = append(pending, m.Id)
	}

	for _, sid := range pending {
		m, err := ctx.getSlotMapping(sid)
		if err != nil {
			return err
		}
		defer s.dirtySlotsCache(m.Id)

		m.Action.State = models.ActionPending
		m.Action.Index = ctx.maxSlotActionIndex() + 1
		m.Action.TargetId = g.Id
		if err := s.storeUpdateSlotMapping(m); err != nil {
			return err
		}
	}
	return nil
}

func (s *Topom) SlotCreateActionRange(beg, end int, gid int, must bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	if !(beg >= 0 && beg <= end && end < MaxSlotNum) {
		return errors.Errorf("invalid slot range [%d,%d]", beg, end)
	}

	g, err := ctx.getGroup(gid)
	if err != nil {
		return err
	}
	if len(g.Servers) == 0 {
		return errors.Errorf("group-[%d] is empty", g.Id)
	}

	var pending []int
	for sid := beg; sid <= end; sid++ {
		m, err := ctx.getSlotMapping(sid)
		if err != nil {
			return err
		}
		if m.Action.State != models.ActionNothing {
			if !must {
				continue
			}
			return errors.Errorf("slot-[%d] action already exists", sid)
		}
		if m.GroupId == g.Id {
			if !must {
				continue
			}
			return errors.Errorf("slot-[%d] already in group-[%d]", sid, g.Id)
		}
		pending = append(pending, m.Id)
	}

	for _, sid := range pending {
		m, err := ctx.getSlotMapping(sid)
		if err != nil {
			return err
		}
		defer s.dirtySlotsCache(m.Id)

		m.Action.State = models.ActionPending
		m.Action.Index = ctx.maxSlotActionIndex() + 1
		m.Action.TargetId = g.Id
		if err := s.storeUpdateSlotMapping(m); err != nil {
			return err
		}
	}
	return nil
}

func (s *Topom) SlotRemoveAction(sid int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	m, err := ctx.getSlotMapping(sid)
	if err != nil {
		return err
	}
	if m.Action.State == models.ActionNothing {
		return errors.Errorf("slot-[%d] action doesn't exist", sid)
	}
	if m.Action.State != models.ActionPending {
		return errors.Errorf("slot-[%d] action isn't pending", sid)
	}
	defer s.dirtySlotsCache(m.Id)

	m = &models.SlotMapping{
		Id:      m.Id,
		GroupId: m.GroupId,
	}
	return s.storeUpdateSlotMapping(m)
}

func (s *Topom) SlotActionPrepare() (int, bool, error) {
	return s.SlotActionPrepareFilter(nil, nil)
}

// slot总共有七种状态，分别是：
// nothing, pending, preparing, prepared, migrating, finished, syncing
func (s *Topom) SlotActionPrepareFilter(accept, update func(m *models.SlotMapping) bool) (int, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return 0, false, err
	}

	// 找到所有Action.State既不是空也不是pending的SlotMapping中Action.Index最小的SlotMapping
	var minActionIndex = func(filter func(m *models.SlotMapping) bool) (picked *models.SlotMapping) {
		for _, m := range ctx.slots {
			if m.Action.State == models.ActionNothing {
				continue
			}
			if filter(m) {
				if picked != nil && picked.Action.Index < m.Action.Index {
					continue
				}
				// 只有一个slot没有执行过Update方法，accept才会返回true；这里保证一个slot只会被处理一次
				if accept == nil || accept(m) {
					picked = m
				}
			}
		}
		return picked
	}

	var m = func() *models.SlotMapping {
		var picked = minActionIndex(func(m *models.SlotMapping) bool {
			return m.Action.State != models.ActionPending
		})
		// 必须不为pending状态
		if picked != nil {
			return picked
		}
		if s.action.disabled.IsTrue() {
			return nil
		}
		// 如果前面找不到Action.State既不为空也不是pending的SlotMapping中Action.Index最小的SlotMapping
		// 就去找到Action.State为pending的SlotMapping中Action.Index最小的SlotMapping
		return minActionIndex(func(m *models.SlotMapping) bool {
			return m.Action.State == models.ActionPending
		})
	}()

	if m == nil {
		return 0, false, nil
	}

	// 找到符合条件的m，并且执行update操作
	if update != nil && !update(m) {
		return 0, false, nil
	}

	log.Warnf("slot-[%d] action prepare:\n%s", m.Id, m.Encode())

	// 变更每个SlotMapping的Action.State，并与zk交互
	// 另外，Action.State符合preparing或者prepared的时候，要根据SlotMapping的参数同步到Slot
	switch m.Action.State {

	case models.ActionPending:

		defer s.dirtySlotsCache(m.Id)

		// Action.State指向下一阶段
		m.Action.State = models.ActionPreparing
		// 更新zk
		if err := s.storeUpdateSlotMapping(m); err != nil {
			return 0, false, err
		}

		fallthrough

	case models.ActionPreparing:

		defer s.dirtySlotsCache(m.Id)

		log.Warnf("slot-[%d] resync to prepared", m.Id)

		m.Action.State = models.ActionPrepared
		// 同步SlotMapping的操作
		if err := s.resyncSlotMappings(ctx, m); err != nil {
			log.Warnf("slot-[%d] resync-rollback to preparing", m.Id)
			m.Action.State = models.ActionPreparing
			s.resyncSlotMappings(ctx, m)
			log.Warnf("slot-[%d] resync-rollback to preparing, done", m.Id)
			return 0, false, err
		}
		// 更新zk
		if err := s.storeUpdateSlotMapping(m); err != nil {
			return 0, false, err
		}

		fallthrough

	case models.ActionPrepared:

		defer s.dirtySlotsCache(m.Id)

		log.Warnf("slot-[%d] resync to migrating", m.Id)

		m.Action.State = models.ActionMigrating
		if err := s.resyncSlotMappings(ctx, m); err != nil {
			log.Warnf("slot-[%d] resync to migrating failed", m.Id)
			return 0, false, err
		}
		if err := s.storeUpdateSlotMapping(m); err != nil {
			return 0, false, err
		}

		fallthrough

	case models.ActionMigrating:

		return m.Id, true, nil

	case models.ActionFinished:

		return m.Id, true, nil

	default:

		return 0, false, errors.Errorf("slot-[%d] action state is invalid", m.Id)

	}
}

func (s *Topom) SlotActionComplete(sid int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	m, err := ctx.getSlotMapping(sid)
	if err != nil {
		return err
	}

	log.Warnf("slot-[%d] action complete:\n%s", m.Id, m.Encode())

	switch m.Action.State {

	// 首先状态是migrating
	case models.ActionMigrating:

		defer s.dirtySlotsCache(m.Id)

		// 推进到ActionFinished
		m.Action.State = models.ActionFinished
		if err := s.storeUpdateSlotMapping(m); err != nil {
			return err
		}

		fallthrough

	case models.ActionFinished:

		log.Warnf("slot-[%d] resync to finished", m.Id)

		if err := s.resyncSlotMappings(ctx, m); err != nil {
			log.Warnf("slot-[%d] resync to finished failed", m.Id)
			return err
		}
		defer s.dirtySlotsCache(m.Id)

		m = &models.SlotMapping{
			Id:      m.Id,
			GroupId: m.Action.TargetId,
		}
		return s.storeUpdateSlotMapping(m)

	default:

		return errors.Errorf("slot-[%d] action state is invalid", m.Id)

	}
}

func (s *Topom) newSlotActionExecutor(sid int) (func(db int) (remains int, nextdb int, err error), error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return nil, err
	}

	// 根据slot的id获取SlotMapping，主要方法就是return ctx.slots[sid], nil
	m, err := ctx.getSlotMapping(sid)
	if err != nil {
		return nil, err
	}

	switch m.Action.State {

	// 最初slot还处于迁移过程中，即migrating
	case models.ActionMigrating:

		if s.action.disabled.IsTrue() {
			return nil, nil
		}
		// 查看m所在的group,如果存在group,而且其Promoting.State不为空字符串，就返回true;否则返回false
		if ctx.isGroupPromoting(m.GroupId) {
			return nil, nil
		}
		if ctx.isGroupPromoting(m.Action.TargetId) {
			return nil, nil
		}

		// 迁移过程中，一个slot本身所在的group以及目标group的Promoting.State都必须为空才可以做迁移
		from := ctx.getGroupMaster(m.GroupId)
		// 取出group2的第一个server，也就是master。
		dest := ctx.getGroupMaster(m.Action.TargetId)

		// Topom的action中的计数器加一
		s.action.executor.Incr()

		return func(db int) (int, int, error) {
			// 每执行一个槽的迁移操作，Topom的action中的计数器就减一
			defer s.action.executor.Decr()
			if from == "" {
				return 0, -1, nil
			}
			// 从cache中得到group1的redisClient,这个client由addr,auth,timeout,Database,redigo.Conn组成；
			// 如果cache没有,就新建。
			c, err := s.action.redisp.GetClient(from)
			if err != nil {
				return 0, -1, err
			}

			// 将刚才新建的或者从cache中取出的redis client再put到Topom.action.redisp中
			defer s.action.redisp.PutClient(c)

			// 这里db是0，相当于redis从16个库中选择0号
			if err := c.Select(db); err != nil {
				return 0, -1, err
			}
			var do func() (int, error)

			method, _ := models.ParseForwardMethod(s.config.MigrationMethod)
			switch method {
			case models.ForwardSync:
				do = func() (int, error) {
					// 调用redis的SLOTSMGRTTAGSLOT命令，随机选择当前slot的一个key，并将与这个key的tag相同的k-v数据全部迁移到目标机
					return c.MigrateSlot(sid, dest)
				}
			case models.ForwardSemiAsync:
				var option = &redis.MigrateSlotAsyncOption{
					MaxBulks: s.config.MigrationAsyncMaxBulks,
					MaxBytes: s.config.MigrationAsyncMaxBytes.AsInt(),
					NumKeys:  s.config.MigrationAsyncNumKeys,
					Timeout: math2.MinDuration(time.Second*5,
						s.config.MigrationTimeout.Duration()),
				}
				do = func() (int, error) {
					// 调用redis的SLOTSMGRTTAGSLOT-ASYNC命令，参数是target redis的ip和port
					return c.MigrateSlotAsync(sid, dest, option)
				}
			default:
				log.Panicf("unknown forward method %d", int(method))
			}

			n, err := do()
			if err != nil {
				return 0, -1, err
			} else if n != 0 {
				return n, db, nil
			}

			nextdb := -1
			// 通过info命令查keyspace信息并做处理，这里取出的m为空
			m, err := c.InfoKeySpace()
			if err != nil {
				return 0, -1, err
			}
			for i := range m {
				if (nextdb == -1 || i < nextdb) && db < i {
					nextdb = i
				}
			}
			return 0, nextdb, nil

		}, nil

	case models.ActionFinished:

		return func(int) (int, int, error) {
			return 0, -1, nil
		}, nil

	default:

		return nil, errors.Errorf("slot-[%d] action state is invalid", m.Id)

	}
}

func (s *Topom) SlotsAssignGroup(slots []*models.SlotMapping) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	for _, m := range slots {
		_, err := ctx.getSlotMapping(m.Id)
		if err != nil {
			return err
		}
		g, err := ctx.getGroup(m.GroupId)
		if err != nil {
			return err
		}
		if len(g.Servers) == 0 {
			return errors.Errorf("group-[%d] is empty", g.Id)
		}
		if m.Action.State != models.ActionNothing {
			return errors.Errorf("invalid slot-[%d] action = %s", m.Id, m.Action.State)
		}
	}

	for i, m := range slots {
		if g := ctx.group[m.GroupId]; !g.OutOfSync {
			defer s.dirtyGroupCache(g.Id)
			g.OutOfSync = true
			if err := s.storeUpdateGroup(g); err != nil {
				return err
			}
		}
		slots[i] = &models.SlotMapping{
			Id: m.Id, GroupId: m.GroupId,
		}
	}

	for _, m := range slots {
		defer s.dirtySlotsCache(m.Id)

		log.Warnf("slot-[%d] will be mapped to group-[%d]", m.Id, m.GroupId)

		if err := s.storeUpdateSlotMapping(m); err != nil {
			return err
		}
	}
	return s.resyncSlotMappings(ctx, slots...)
}

func (s *Topom) SlotsAssignOffline(slots []*models.SlotMapping) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	for _, m := range slots {
		_, err := ctx.getSlotMapping(m.Id)
		if err != nil {
			return err
		}
		if m.GroupId != 0 {
			return errors.Errorf("group of slot-[%d] should be 0", m.Id)
		}
	}

	for i, m := range slots {
		slots[i] = &models.SlotMapping{
			Id: m.Id,
		}
	}

	for _, m := range slots {
		defer s.dirtySlotsCache(m.Id)

		log.Warnf("slot-[%d] will be mapped to group-[%d] (offline)", m.Id, m.GroupId)

		if err := s.storeUpdateSlotMapping(m); err != nil {
			return err
		}
	}
	return s.resyncSlotMappings(ctx, slots...)
}

// 第一次进入的时候传入的confirm参数为false，因为只是知道rebalance的plan。当页面上弹窗问是否迁移的时候
// 点了OK，又会进入这个方法，传入confirm为true

// 另外还有手动迁移，主要有：1.指定序号的slot移到某个group，二是将一个group中的多少个slot移动到另一个group，主要流程类似，
// 先取出当前集群的上下文，然后根据请求参数做校验，将符合迁移条件的slot放到一个pending切片里面，接下来更新zk。
func (s *Topom) SlotsRebalance(confirm bool) (map[int]int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return nil, err
	}

	var groupIds []int
	for _, g := range ctx.group {
		if len(g.Servers) != 0 {
			groupIds = append(groupIds, g.Id)
		}
	}

	// 升序排序
	// step1:将groupIds切片升序排列
	sort.Ints(groupIds)

	if len(groupIds) == 0 {
		return nil, errors.Errorf("no valid group could be found")
	}

	// step2：划分各个切片：已分配好的，等待分配的，可以迁出的，确定要迁移的id切片
	var (
		// 已分配好，不需要再迁移的，key为groupId，value为当前group不需迁移的slot的数量
		assigned = make(map[int]int)
		// 等待分配的，key为groupId，value为当前group等待分配的slot的id组成的切片
		pendings = make(map[int][]int)
		// 可以迁出的，key为groupId，value为当前group可以迁出的slot的数量。如果是负数，就表明当前group需要迁入多少个slot
		moveout = make(map[int]int)
		// 确定要迁移的slot，int切片，每个元素就是确定要迁移的slot的id
		docking []int
	)

	// 计算某个group中槽的数量的方法 = 固定属于该组的 + 待分配的 - 要迁移出去的
	var groupSize = func(gid int) int {
		return assigned[gid] + len(pendings[gid]) - moveout[gid]
	}

	// don't migrate slot if it's being migrated
	// 如果槽的Action.State为空，才可以迁移。否则，不能进行迁移
	for _, m := range ctx.slots {
		if m.Action.State != models.ActionNothing {
			// 复制assigned
			assigned[m.Action.TargetId]++
		}
	}

	// 计算平均每个group占的slot数目下界
	var lowerBound = MaxSlotNum / len(groupIds)

	// don't migrate slot if groupSize < lowerBound
	// 遍历槽，如果槽所属的group的size少于lowerBound，这个槽也不需要迁移
	// step3：遍历槽，如果槽所属于的group负责的槽数过少，不予分配，否则，塞到可以分配的列表里面
	for _, m := range ctx.slots {
		if m.Action.State != models.ActionNothing {
			continue
		}
		if m.GroupId != 0 {
			if groupSize(m.GroupId) < lowerBound {
				assigned[m.GroupId]++
			} else {
				// 表示当前槽可以等待分配
				// 代表：该group（m.GroupId)的槽m.Id可以分配走了
				pendings[m.GroupId] = append(pendings[m.GroupId], m.Id)
			}
		}
	}

	// 传入一个自定义的比较器，新建红黑树。这个比较器的结果是，红黑树中左边的节点的groupSize小于右边的。
	// step4:建立红黑树
	var tree = rbtree.NewWith(func(x, y interface{}) int {
		var gid1 = x.(int)
		var gid2 = y.(int)
		if gid1 != gid2 {
			if d := groupSize(gid1) - groupSize(gid2); d != 0 {
				return d
			}
			return gid1 - gid2
		}
		return 0
	})
	for _, gid := range groupIds {
		tree.Put(gid, nil)
	}

	// assign offline slots to the smallest group
	// 将不属于任何group和Action.State为models.ActionNothing的slot（即被称为offline的slot，初始阶段所有slot都是offline）
	// 分配给目前size最小的group
	// step5：确定可以迁移的id，以及要迁到那个目的地中（注意：这里收集的是不属于任何group的slot（if m.GroupId != 0)，不同于上面）
	for _, m := range ctx.slots {
		if m.Action.State != models.ActionNothing {
			continue
		}
		// 注意这里，跟上面不同
		if m.GroupId != 0 {
			continue
		}
		// 得到整个树最左边节点的键，也就是size最小的group的id
		dest := tree.Left().Key.(int)
		tree.Remove(dest)

		// 当前节点需要进行迁移
		docking = append(docking, m.Id)
		moveout[dest]--

		tree.Put(dest, nil)
	}

	var upperBound = (MaxSlotNum + len(groupIds) - 1) / len(groupIds)

	// rebalance between different server groups
	// 当集群中的group数量大于2的时候，执行红黑树的rebalance。
	// 在group size差距最大的两个组之间做迁移准备工作。
	// from和dest分别是红黑树最左和最右的两个group，换句话说，slot之间的补给传递，都是先比较当前groupsize最大和最小的组
	// step6：计算某个槽要往里面迁移多少个slot
	for tree.Size() >= 2 {
		// group size最大的groupId
		from := tree.Right().Key.(int)
		tree.Remove(from)

		if len(pendings[from]) == moveout[from] {
			continue
		}
		dest := tree.Left().Key.(int)
		tree.Remove(dest)

		var (
			fromSize = groupSize(from)
			destSize = groupSize(dest)
		)

		// 让所有的节点保持平衡（位于[lowerBound, upperBound]之间）
		if fromSize <= lowerBound {
			break
		}
		if destSize >= upperBound {
			break
		}
		if d := fromSize - destSize; d <= 1 {
			break
		}
		moveout[from]++
		moveout[dest]--

		tree.Put(from, nil)
		tree.Put(dest, nil)
	}

	// 把pendings的数据迁移过去
	for gid, n := range moveout {
		if n < 0 {
			continue
		}
		if n > 0 {
			sids := pendings[gid]
			sort.Sort(sort.Reverse(sort.IntSlice(sids)))

			// 全部数据塞到docking里面
			docking = append(docking, sids[0:n]...)
			pendings[gid] = sids[n:]
		}
		delete(moveout, gid)
	}

	// docking切片升序排列
	sort.Ints(docking)

	// 建立映射关系：key是slot的id，value是这个slot要迁移到的group的Id
	var plans = make(map[int]int)

	// 找到需要迁入slot的group，也就是moveout[gid]为负数的group，从docking中的第一个元素开始迁移到这个group
	for _, gid := range groupIds {
		var in = -moveout[gid]
		for i := 0; i < in && len(docking) != 0; i++ {
			plans[docking[0]] = gid
			// docking去除刚刚分配了的首元素
			docking = docking[1:]
		}
	}

	if !confirm {
		return plans, nil
	}

	// 只有弹窗点击OK，方法才会走到这里，下面的逻辑开始执行plan中的步骤
	var slotIds []int
	for sid, _ := range plans {
		slotIds = append(slotIds, sid)
	}
	sort.Ints(slotIds)

	for _, sid := range slotIds {
		m, err := ctx.getSlotMapping(sid)
		if err != nil {
			return nil, err
		}
		defer s.dirtySlotsCache(m.Id)

		m.Action.State = models.ActionPending
		// 每一个Slot的Action.Index都是其slotId + 1
		m.Action.Index = ctx.maxSlotActionIndex() + 1
		m.Action.TargetId = plans[sid] // 修改这个slot的目标ID
		// 更新zk
		if err := s.storeUpdateSlotMapping(m); err != nil {
			return nil, err
		}
	}
	return plans, nil
}
