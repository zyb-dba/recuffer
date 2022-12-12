// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/docopt/docopt-go"

	"github.com/go-redis/redis"
	// 调试问题使用
	// _ "go.uber.org/automaxprocs"

	// "net/http"
	// _ "net/http/pprof"

	"github.com/recuffer/pkg/models"
	"github.com/recuffer/pkg/proxy"
	"github.com/recuffer/pkg/topom"
	"github.com/recuffer/pkg/utils"
	"github.com/recuffer/pkg/utils/log"
	"github.com/recuffer/pkg/utils/math2"
)

/*
启动过程：
读取配置文件，获取Config对象。根据Config对象新建Proxy，填充Proxy的各个属性，这里面比较重要的是填充models.Proxy（因为详细信息
可以在zk中查看），并且与zk连接，注册相关路径。启动goroutine监听11080端口的codis集群发过来的请求并进行转发，以及监听发到19000端口的redis
请求并进行处理。
*/
func main() {
	const usage = `
Usage:
	recuffer [--ncpu=N [--max-ncpu=MAX]] [--config=CONF] [--log=FILE] [--proxy_type=rediscluster] [--log-level=LEVEL] [--host-admin=ADDR] [--host-proxy=ADDR] [--dashboard=ADDR|--zookeeper=ADDR [--zookeeper-auth=USR:PWD]|--etcd=ADDR [--etcd-auth=USR:PWD]|--filesystem=ROOT|--fillslots=FILE] [--ulimit=NLIMIT] [--pidfile=FILE] [--product_name=NAME] [--product_auth=AUTH] [--session_auth=AUTH]
	recuffer  --default-config
	recuffer  --version

Options:
	--ncpu=N                    set runtime.GOMAXPROCS to N, default is runtime.NumCPU().
	-c CONF, --config=CONF      run with the specific configuration.
	-l FILE, --log=FILE         set path/name of daliy rotated log file.
	--log-level=LEVEL           set the log-level, should be INFO,WARN,DEBUG or ERROR, default is INFO.
	--ulimit=NLIMIT             run 'ulimit -n' to check the maximum number of open file descriptors.
`

	d, err := docopt.Parse(usage, nil, true, "", false)
	if err != nil {
		log.PanicError(err, "parse arguments failed")
	}

	switch {

	case d["--default-config"]:
		fmt.Println(proxy.DefaultConfig)
		return

	case d["--version"].(bool):
		fmt.Println("version:", utils.Version)
		fmt.Println("compile:", utils.Compile)
		return

	}

	if s, ok := utils.Argument(d, "--log"); ok {
		w, err := log.NewRollingFile(s, log.DailyRolling)
		if err != nil {
			log.PanicErrorf(err, "open log file %s failed", s)
		} else {
			log.StdLog = log.New(w, "")
		}
	}
	log.SetLevel(log.LevelInfo)

	if s, ok := utils.Argument(d, "--log-level"); ok {
		if !log.SetLevelString(s) {
			log.Panicf("option --log-level = %s", s)
		}
	}

	if n, ok := utils.ArgumentInteger(d, "--ulimit"); ok {
		b, err := exec.Command("/bin/sh", "-c", "ulimit -n").Output()
		if err != nil {
			log.PanicErrorf(err, "run ulimit -n failed")
		}
		if v, err := strconv.Atoi(strings.TrimSpace(string(b))); err != nil || v < n {
			log.PanicErrorf(err, "ulimit too small: %d, should be at least %d", v, n)
		}
	}

	var ncpu int
	if n, ok := utils.ArgumentInteger(d, "--ncpu"); ok {
		ncpu = n
	} else {
		ncpu = 4
	}
	runtime.GOMAXPROCS(ncpu)

	var maxncpu int
	if n, ok := utils.ArgumentInteger(d, "--max-ncpu"); ok {
		maxncpu = math2.MaxInt(ncpu, n)
	} else {
		maxncpu = math2.MaxInt(ncpu, runtime.NumCPU())
	}
	log.Warnf("set ncpu = %d, max-ncpu = %d", ncpu, maxncpu)

	if ncpu < maxncpu {
		go AutoGOMAXPROCS(ncpu, maxncpu)
	}

	config := proxy.NewDefaultConfig()
	if s, ok := utils.Argument(d, "--config"); ok {
		if err := config.LoadFromFile(s); err != nil {
			log.PanicErrorf(err, "load config %s failed", s)
		}
	}
	if s, ok := utils.Argument(d, "--host-admin"); ok {
		config.HostAdmin = s
		log.Warnf("option --host-admin = %s", s)
	}
	if s, ok := utils.Argument(d, "--host-proxy"); ok {
		config.HostProxy = s
		log.Warnf("option --host-proxy = %s", s)
	}

	var dashboard string
	if s, ok := utils.Argument(d, "--dashboard"); ok {
		dashboard = s
		log.Warnf("option --dashboard = %s", s)
	}

	var coordinator struct {
		name string
		addr string
		auth string
	}

	switch {

	case d["--zookeeper"] != nil:
		coordinator.name = "zookeeper"
		coordinator.addr = utils.ArgumentMust(d, "--zookeeper")
		if d["--zookeeper-auth"] != nil {
			coordinator.auth = utils.ArgumentMust(d, "--zookeeper-auth")
		}

	case d["--etcd"] != nil:
		coordinator.name = "etcd"
		coordinator.addr = utils.ArgumentMust(d, "--etcd")
		if d["--etcd-auth"] != nil {
			coordinator.auth = utils.ArgumentMust(d, "--etcd-auth")
		}

	case d["--filesystem"] != nil:
		coordinator.name = "filesystem"
		coordinator.addr = utils.ArgumentMust(d, "--filesystem")

	}

	if coordinator.name != "" {
		log.Warnf("option --%s = %s", coordinator.name, coordinator.addr)
	}

	var proxy_type string
	if s, ok := utils.Argument(d, "--proxy_type"); ok {
		proxy_type = s
		log.Warnf("option --proxy_type = %s", s)
	}

	var slots []*models.Slot
	if s, ok := utils.Argument(d, "--fillslots"); ok {
		b, err := ioutil.ReadFile(s)
		if err != nil {
			log.PanicErrorf(err, "load slots from file failed")
		}
		if err := json.Unmarshal(b, &slots); err != nil {
			log.PanicErrorf(err, "decode slots from json failed")
		}
	}

	if s, ok := utils.Argument(d, "--product_name"); ok {
		config.ProductName = s
		log.Warnf("option --product_name = %s", s)
	}
	if s, ok := utils.Argument(d, "--product_auth"); ok {
		config.ProductAuth = s
		log.Warnf("option --product_auth = %s", s)
	}
	if s, ok := utils.Argument(d, "--session_auth"); ok {
		config.SessionAuth = s
		log.Warnf("option --session_auth = %s", s)
	}

	// 新建proxy对象
	s, err := proxy.New(config)
	if err != nil {
		log.PanicErrorf(err, "create proxy with config file failed\n%s", config)
	}
	defer s.Close()

	log.Warnf("create proxy with config\n%s", config)

	if s, ok := utils.Argument(d, "--pidfile"); ok {
		if pidfile, err := filepath.Abs(s); err != nil {
			log.WarnErrorf(err, "parse pidfile = '%s' failed", s)
		} else if err := ioutil.WriteFile(pidfile, []byte(strconv.Itoa(os.Getpid())), 0644); err != nil {
			log.WarnErrorf(err, "write pidfile = '%s' failed", pidfile)
		} else {
			defer func() {
				if err := os.Remove(pidfile); err != nil {
					log.WarnErrorf(err, "remove pidfile = '%s' failed", pidfile)
				}
			}()
			log.Warnf("option --pidfile = %s", pidfile)
		}
	}

	go func() {
		defer s.Close()
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGINT, syscall.SIGKILL, syscall.SIGTERM)

		sig := <-c
		log.Warnf("[%p] proxy receive signal = '%v'", s, sig)
	}()

	// 由于通常启动的时候，配置文件中的MetircsReporterServer,MetircsInfluxdbServer和MetircsStatsdPeriod三个字段都为空
	// 所以实际上什么都没做
	switch {
	case dashboard != "":
		go AutoOnlineWithDashboard(s, dashboard)
	case coordinator.name != "":
		go AutoOnlineWithCoordinator(s, coordinator.name, coordinator.addr, coordinator.auth)
	case slots != nil: // 这里仅适用于把由原来的槽作为启动参数导入进来
		/*
			proxy启动之后，一直处于waiting的状态，直到将proxy添加到集群。首先对要添加的proxy做参数校验，根据proxy addr获取一个
			ApiClient，更新zk中有关于这个proxy的信息。接下来，从上下文中获取ctx.slots，也就是[]*models.SlotMapping，创建
			1024个models.Slot，再填充1024个pkg/proxy/slots.go中的Slot,此过程中Router为每个Slot都分配了对应的backendConn。

			下一步，将Proxy自身，和它的router和jodis设为上线，在zk中创建临时节点/jodis/codis-productName/proxy-token，并监听
			该节点的变化，如果有变化从相应的channel众读出值。启动dashboard的时候，有一个goroutine专门负责刷新proxy状态，将每一个
			proxy.Token和ProxyStats的关系关联起来，存入Topom.stats.proxies中。
		*/
		go AutoOnlineWithFillSlots(s, slots)
	case proxy_type == "rediscluster":
		// 启动类型是rediscluster的话，获取cluster的slots信息填充
		go AutoOnlineWithClusterSlots(config, s)
	}

	for !s.IsClosed() && !s.IsOnline() {
		log.Warnf("[%p] proxy waiting online ...", s)
		time.Sleep(time.Second)
	}

	log.Warnf("[%p] proxy is working ...", s)

	for !s.IsClosed() {
		time.Sleep(time.Second)
	}

	log.Warnf("[%p] proxy is exiting ...", s)
}

func AutoGOMAXPROCS(min, max int) {
	for {
		var ncpu = runtime.GOMAXPROCS(0)
		var less, more int
		var usage [10]float64
		for i := 0; i < len(usage) && more == 0; i++ {
			u, _, err := utils.CPUUsage(time.Second)
			if err != nil {
				log.WarnErrorf(err, "get cpu usage failed")
				time.Sleep(time.Second * 30)
				continue
			}
			u /= float64(ncpu)
			switch {
			case u < 0.30 && ncpu > min:
				less++
			case u > 0.70 && ncpu < max:
				more++
			}
			usage[i] = u
		}
		var nn = ncpu
		switch {
		case more != 0:
			nn = ncpu + ((max - ncpu + 3) / 4)
		case less == len(usage):
			nn = ncpu - 1
		}
		if nn != ncpu {
			runtime.GOMAXPROCS(nn)
			var b bytes.Buffer
			for i, u := range usage {
				if i != 0 {
					fmt.Fprintf(&b, ", ")
				}
				fmt.Fprintf(&b, "%.3f", u)
			}
			log.Warnf("ncpu = %d -> %d, usage = [%s]", ncpu, nn, b.Bytes())
		}
	}
}

func AutoOnlineWithDashboard(p *proxy.Proxy, dashboard string) {
	for i := 0; i < 10; i++ {
		if p.IsClosed() || p.IsOnline() {
			return
		}
		if OnlineProxy(p, dashboard) {
			return
		}
		time.Sleep(time.Second * 3)
	}
	log.Panicf("online proxy failed")
}

func AutoOnlineWithCoordinator(p *proxy.Proxy, name, addr, auth string) {
	client, err := models.NewClient(name, addr, auth, time.Minute)
	if err != nil {
		log.PanicErrorf(err, "create '%s' client to '%s' failed", name, addr)
	}
	defer client.Close()
	for i := 0; i < 30; i++ {
		if p.IsClosed() || p.IsOnline() {
			return
		}
		t, err := models.LoadTopom(client, p.Config().ProductName, false)
		if err != nil {
			log.WarnErrorf(err, "load & decode topom failed")
		} else if t != nil && OnlineProxy(p, t.AdminAddr) {
			return
		}
		time.Sleep(time.Second * 3)
	}
	log.Panicf("online proxy failed")
}

func lookupIPAddr(addr string) net.IP {
	tcpAddr := utils.ResolveTCPAddrTimeout(addr, 50*time.Millisecond)
	if tcpAddr != nil {
		return tcpAddr.IP
	} else {
		return nil
	}

}

// 根据读流量黑名单配置，将黑名单的节点排除，获取需要加流量的节点
func removeReadBlackList(c *proxy.Config, p *proxy.Proxy, masterNodes map[string]string, slotsNodes []string, isOnline bool) []string {
	var newSlotsNodes []string
	check_addr_error := errors.New("Conf ReadBlackList Format Error.")
	if isOnline {
		if len(c.ReadBlackList) > 0 {
			readBlackList := strings.Split(c.ReadBlackList, ",")
			if len(readBlackList) > 0 {
				for _, v := range readBlackList {
					if utils.CheckAddr(v) {
						if _, ok := masterNodes[v]; !ok {
							p.SetReadBlackList(v)
						}
					} else {
						log.PanicErrorf(check_addr_error, "ReadBlackList need redis addr:port.")
					}
				}
			}
		}
	}
	newReadBlackList := p.GetReadBlackList()
	if len(newReadBlackList) > 0 {
		for _, vv := range slotsNodes {
			_, ok := masterNodes[vv]
			if utils.SliceHasStr(vv, newReadBlackList) && !ok {
				continue
			} else {
				newSlotsNodes = append(newSlotsNodes, vv)
			}
		}
	}
	if len(newSlotsNodes) > 0 {
		return newSlotsNodes
	} else {
		return slotsNodes
	}
}

// 指定IP段读优先时匹配机房使用的功能，通过给定的ip段字符串，进行匹配
func matchDataCenter(datacenter string, addr string) bool {
	splitIp := strings.Split(datacenter, ",")
	for _, v := range splitIp {
		addrRule := "^" + v
		addrReg := regexp.MustCompile(addrRule)
		isMatchAddr := addrReg.MatchString(addr)
		if isMatchAddr {
			return true
		}
	}
	return false
}

// 通过参数文件配置，获取replica_groups的优先级
func getReplicaGroupsPriority(p *proxy.Proxy, replicaGroups [][]string) [][]string {
	// 配置文件配置了datacenter的话，优先根据datacenter判断访问优先级
	// 获取本机地址
	localIp := lookupIPAddr(p.AdminAddr())
	if localIp != nil {
		localIpZone := strings.Split(localIp.String(), ".")[0]
		// datacenter为空的话，replica的访问优先级通过ip自动判断，主要根据本机地址，判断
		// 后端节点与本机IP地址一致，优先路由到本机IP，同一网段次之，其他节点优先级最低
		getPriorityAuto := func(bcAddr string) int {
			bcAddrZone := strings.Split(bcAddr, ".")[0]
			if localIp.String() == bcAddr {
				return 0
			} else if localIpZone == bcAddrZone {
				return 1
			} else {
				return 2
			}
		}
		// replica的访问优先级通过配置文件datacenter判断
		// datacenter设置的话，replica的访问优先级通过与datacenter的配置对比
		// 后端节点与本机IP地址一致，优先路由到本机IP，符合datacenter配置的节点次之，其他节点优先级最低
		getPriorityDataCenter := func(bcAddr string) int {
			if localIp.String() == bcAddr {
				return 0
			} else if matchDataCenter(p.Config().ProxyDataCenter, bcAddr) {
				return 1
			} else {
				return 2
			}
		}
		var groups [3][]string
		for _, v := range replicaGroups[0] {
			var serverPriority int
			if len(p.Config().ProxyDataCenter) > 0 {
				serverPriority = getPriorityDataCenter(strings.Split(v, ":")[0])
			} else {
				serverPriority = getPriorityAuto(strings.Split(v, ":")[0])
			}
			groups[serverPriority] = append(groups[serverPriority], v)
		}
		var replicas [][]string
		for _, l := range groups {
			if len(l) != 0 {
				replicas = append(replicas, l)
			}
		}
		return replicas
	} else {
		log.Warnf("get local ip fail.")
		return replicaGroups
	}
}

var client *redis.ClusterClient

// 通过配置的集群地址，获取rediscluster的slot信息，转化为proxy的models.Slot
func getClusterSlots(c *proxy.Config, p *proxy.Proxy, isOnline bool) []*models.Slot {
	var slots []*models.Slot
	allNodes := make(map[string]string)
	masterNodes := make(map[string]string)
	clusterAddr := strings.Split(c.ClusterAddr, ",")
	// 检测配置文件地址正确性，不正确则不启动
	if len(clusterAddr) > 0 {
		for _, v := range clusterAddr {
			if !utils.CheckAddr(v) {
				log.Panicf("config cluster_addr %v is not redis addr, please check.", v)
			}
		}
	} else {
		log.Panicf("config cluster_addr is empty, please check.")
	}
	client = redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:              clusterAddr, //set redis cluster url
		PoolSize:           1,
		IdleCheckFrequency: -1,
	})
	clusterSlots, err := client.ClusterSlots().Result()
	if err != nil {
		log.Warnf("get cluster slots failed, %v", err)
	}
	// 获取cluster slots后关闭连接
	defer client.Close()
	if len(clusterSlots) > 0 {
		for _, v := range clusterSlots {
			for _, vvv := range v.Nodes {
				if allNodes != nil {
					allNodes[vvv.Addr] = vvv.Addr
				}
				p.SetNodes(vvv.Addr)
			}
			// log.Warnf("getClusterSlots v:%v", v)
			backend_addr := v.Nodes[0].Addr
			if masterNodes != nil {
				masterNodes[backend_addr] = backend_addr
			}
			for i := v.Start; i <= v.End; i++ {
				id := i
				backend_addr_group_id := i
				var replica_groups [][]string
				var slots_nodes []string
				sort.SliceStable(v.Nodes, func(i int, j int) bool {
					return v.Nodes[i].Id < v.Nodes[j].Id
				})
				for _, vv := range v.Nodes {
					slots_nodes = append(slots_nodes, vv.Addr)
				}
				// 判断是否添加的有读流量黑名单
				newSlotsNodes := removeReadBlackList(c, p, masterNodes, slots_nodes, isOnline)
				replica_groups = append(replica_groups, newSlotsNodes)
				slot_data := &models.Slot{Id: id, BackendAddr: backend_addr, BackendAddrGroupId: backend_addr_group_id, ReplicaGroups: replica_groups}
				slots = append(slots, slot_data)
			}
		}
		// 删除s.nodes中不存在的节点信息 及清除掉对应的 moved pool
		if len(allNodes) > 0 {
			p.DelNodes(allNodes)
			p.DelMovedPool()
		}

	}

	return slots
}

// // 设置访问优先级
func setSlotsReplicaGroupsPriority(p *proxy.Proxy, slots []*models.Slot) []*models.Slot {
	var handledSlots []*models.Slot
	if len(slots) > 0 {
		for _, v := range slots {
			replicaGroupsPriority := getReplicaGroupsPriority(p, v.ReplicaGroups)
			slot_data := &models.Slot{Id: v.Id, Locked: v.Locked, BackendAddr: v.BackendAddr, BackendAddrGroupId: v.BackendAddrGroupId, MigrateFrom: v.MigrateFrom, ForwardMethod: v.ForwardMethod, ReplicaGroups: replicaGroupsPriority}
			handledSlots = append(handledSlots, slot_data)

		}
	}
	return handledSlots
}

// 指定参数启动支持rediscluster模式
func AutoOnlineWithClusterSlots(c *proxy.Config, p *proxy.Proxy) {
	// 调试问题使用
	// go func() {
	// 	log.Println(http.ListenAndServe("0.0.0.0:8500", nil))
	// }()
	slots := getClusterSlots(c, p, true)
	handledSlots := setSlotsReplicaGroupsPriority(p, slots)
	if len(handledSlots) > 0 {
		if err := p.FillSlots(handledSlots); err != nil {
			log.PanicErrorf(err, "fill slots failed")
		}
		if err := p.Start(); err != nil {
			log.PanicErrorf(err, "start proxy failed")
		}
		go RefreshSlots(slots, c, p)
	} else {
		log.Panic("slots is empty, start proxy failed.")
	}

}

// 对比刷新slots信息，监听rediscluster的slot信息变化，刷新proxy的slot信息
func RefreshSlots(slots []*models.Slot, c *proxy.Config, p *proxy.Proxy) {
	var INTERVAL = 1 * time.Second
	for {
		newSlots := getClusterSlots(c, p, false)
		if len(newSlots) > 0 {
			sort.SliceStable(newSlots, func(i int, j int) bool {
				return newSlots[i].Id < newSlots[j].Id
			})
			sort.SliceStable(slots, func(i int, j int) bool {
				return slots[i].Id < slots[j].Id
			})
			isDiff := reflect.DeepEqual(newSlots, slots)
			if !isDiff {
				slots = newSlots
				log.Warnf("slots has change, need refresh slots info")
				handledSlots := setSlotsReplicaGroupsPriority(p, slots)
				if len(handledSlots) > 0 {
					if err := p.FillSlots(handledSlots); err != nil {
						log.PanicErrorf(err, "fill slots failed")
					}
				}

			}
		}
		time.Sleep(INTERVAL)
	}
}

func AutoOnlineWithFillSlots(p *proxy.Proxy, slots []*models.Slot) {
	if err := p.FillSlots(slots); err != nil {
		log.PanicErrorf(err, "fill slots failed")
	}
	if err := p.Start(); err != nil {
		log.PanicErrorf(err, "start proxy failed")
	}
}

func OnlineProxy(p *proxy.Proxy, dashboard string) bool {
	client := topom.NewApiClient(dashboard)
	t, err := client.Model()
	if err != nil {
		log.WarnErrorf(err, "rpc fetch model failed")
		return false
	}
	if t.ProductName != p.Config().ProductName {
		log.Panicf("unexcepted product name, got model =\n%s", t.Encode())
	}
	client.SetXAuth(p.Config().ProductName)

	if err := client.OnlineProxy(p.Model().AdminAddr); err != nil {
		log.WarnErrorf(err, "rpc online proxy failed")
		return false
	} else {
		log.Warnf("rpc online proxy seems OK")
		return true
	}
}
