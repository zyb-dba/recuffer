// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package proxy

import (
	"sync"
	"unsafe"

	"github.com/recuffer/pkg/proxy/redis"
	"github.com/recuffer/pkg/utils/sync2/atomic2"
)

type Request struct {
	Multi []*redis.Resp
	Batch *sync.WaitGroup
	Group *sync.WaitGroup

	Broken *atomic2.Bool

	OpStr string
	OpFlag

	Database int32
	UnixNano int64

	*redis.Resp
	Err error

	Coalesce func() error
}

func (r *Request) IsBroken() bool {
	return r.Broken != nil && r.Broken.IsTrue()
}

func (r *Request) MakeSubRequest(n int) []Request {
	var sub = make([]Request, n)
	for i := range sub {
		x := &sub[i]
		x.Batch = r.Batch
		x.OpStr = r.OpStr
		x.OpFlag = r.OpFlag
		x.Broken = r.Broken
		x.Database = r.Database
		x.UnixNano = r.UnixNano
	}
	return sub
}

const GOLDEN_RATIO_PRIME_32 = 0x9e370001

func (r *Request) Seed16() uint {
	h32 := uint32(r.UnixNano) + uint32(uintptr(unsafe.Pointer(r)))
	h32 *= GOLDEN_RATIO_PRIME_32
	return uint(h32 >> 16)
}

type RequestChan struct {
	lock sync.Mutex

	// sync.NewCond(&RequestChan.lock)
	// 如果RequestChan为空，就让goroutine阻塞；如果向RequestChan放入了一个请求，并且有goroutine在等待了，就唤醒一个
	cond *sync.Cond

	data []*Request
	buff []*Request

	waits  int
	closed bool
}

const DefaultRequestChanBuffer = 128

func NewRequestChan() *RequestChan {
	return NewRequestChanBuffer(0)
}

func NewRequestChanBuffer(n int) *RequestChan {
	if n <= 0 {
		n = DefaultRequestChanBuffer
	}
	var ch = &RequestChan{
		buff: make([]*Request, n),
	}
	ch.cond = sync.NewCond(&ch.lock)
	return ch
}

func (c *RequestChan) Close() {
	c.lock.Lock()
	if !c.closed {
		c.closed = true
		c.cond.Broadcast()
	}
	c.lock.Unlock()
}

func (c *RequestChan) Buffered() int {
	c.lock.Lock()
	n := len(c.data)
	c.lock.Unlock()
	return n
}

func (c *RequestChan) PushBack(r *Request) int {
	c.lock.Lock()
	n := c.lockedPushBack(r)
	c.lock.Unlock()
	return n
}

func (c *RequestChan) PopFront() (*Request, bool) {
	c.lock.Lock()
	r, ok := c.lockedPopFront()
	c.lock.Unlock()
	return r, ok
}

func (c *RequestChan) lockedPushBack(r *Request) int {
	if c.closed {
		panic("send on closed chan")
	}
	// RequestChan的waits标识不为0的时候（也就是在RequestChan上等待的request数量不为0时），唤醒一个在cond上等待的goroutine
	// 这里的意思是，如果想RequestChan中放入了情趣，就将一个在cond上等待取出的goroutine唤醒。
	if c.waits != 0 {
		c.cond.Signal()
	}
	// 将request添加到RequestChan的data([] *Request)切片中，用于记录处理过的请求。
	c.data = append(c.data, r)
	return len(c.data)
}

func (c *RequestChan) lockedPopFront() (*Request, bool) {
	for len(c.data) == 0 {
		if c.closed {
			return nil, false
		}
		c.data = c.buff[:0]
		c.waits++
		c.cond.Wait()
		c.waits--
	}
	var r = c.data[0]
	c.data, c.data[0] = c.data[1:], nil
	return r, true
}

func (c *RequestChan) IsEmpty() bool {
	return c.Buffered() == 0
}

func (c *RequestChan) PopFrontAll(onRequest func(r *Request) error) error {
	for {
		r, ok := c.PopFront()
		if ok {
			if err := onRequest(r); err != nil {
				return err
			}
		} else {
			return nil
		}
	}
}

func (c *RequestChan) PopFrontAllVoid(onRequest func(r *Request)) {
	c.PopFrontAll(func(r *Request) error {
		onRequest(r)
		return nil
	})
}
