module github.com/recuffer

go 1.17

replace github.com/coreos/bbolt v1.3.6 => go.etcd.io/bbolt v1.3.6

require (
	github.com/BurntSushi/toml v0.4.1
	github.com/coreos/etcd v3.3.26+incompatible
	github.com/docopt/docopt-go v0.0.0-20180111231733-ee0de3bc6815
	github.com/emirpasic/gods v1.12.0
	github.com/garyburd/redigo v1.6.2
	github.com/go-martini/martini v0.0.0-20170121215854-22fa46961aab
	github.com/go-redis/redis v6.15.9+incompatible
	github.com/influxdata/influxdb v1.9.5
	github.com/martini-contrib/binding v0.0.0-20160701174519-05d3e151b6cf
	github.com/martini-contrib/gzip v0.0.0-20151124214156-6c035326b43f
	github.com/martini-contrib/render v0.0.0-20150707142108-ec18f8345a11
	github.com/samuel/go-zookeeper v0.0.0-20201211165307-7117e9ea2414
	github.com/spinlock/jemalloc-go v0.0.0-20201010032256-e81523fb8524
	go.uber.org/automaxprocs v1.4.0
	golang.org/x/net v0.0.0-20211007125505-59d4e928ea9d
	gopkg.in/alexcesaro/statsd.v2 v2.0.0
)

require (
	github.com/codegangsta/inject v0.0.0-20150114235600-33e0aa1cb7c0 // indirect
	github.com/coreos/bbolt v1.3.6 // indirect
	github.com/coreos/go-semver v0.2.0 // indirect
	github.com/json-iterator/go v1.1.11 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.1 // indirect
	github.com/oxtoacart/bpool v0.0.0-20190530202638-03653db5a59c // indirect
	github.com/prometheus/client_golang v1.11.0 // indirect
	github.com/sirupsen/logrus v1.8.1 // indirect
)
