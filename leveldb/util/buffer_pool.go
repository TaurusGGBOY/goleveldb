// Copyright (c) 2014, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package util

import (
	"fmt"
	"sync"
	"sync/atomic"
)

// BufferPool is a 'buffer pool'.
type BufferPool struct {
	pool     [6]sync.Pool
	baseline [5]int

	// 这些都是统计数据
	get     uint32
	put     uint32
	less    uint32
	equal   uint32
	greater uint32
	miss    uint32
}

func (p *BufferPool) poolNum(n int) int {
	// 看这个缓存的字节的长度 是哪个基线里面的
	for i, x := range p.baseline {
		if n <= x {
			return i
		}
	}
	// 如果最大 就是最后一个
	return len(p.baseline)
}

// Get returns buffer with length of n.
func (p *BufferPool) Get(n int) []byte {
	if p == nil {
		return make([]byte, n)
	}
	atomic.AddUint32(&p.get, 1)

	poolNum := p.poolNum(n)

	// 池子里面拿n个字节
	b := p.pool[poolNum].Get().(*[]byte)

	// 没命中
	if cap(*b) == 0 {
		// If we grabbed nothing, increment the miss stats.
		atomic.AddUint32(&p.miss, 1)

		if poolNum == len(p.baseline) {
			*b = make([]byte, n)
			return *b
		}

		*b = make([]byte, p.baseline[poolNum])
		*b = (*b)[:n]
		return *b
		// 命中了
	} else {
		// If there is enough capacity in the bytes grabbed, resize the length
		// to n and return.
		if n < cap(*b) {
			atomic.AddUint32(&p.less, 1)
			*b = (*b)[:n]
			return *b
		} else if n == cap(*b) {
			atomic.AddUint32(&p.equal, 1)
			*b = (*b)[:n]
			return *b
			// 要的太大了
		} else if n > cap(*b) {
			atomic.AddUint32(&p.greater, 1)
		}
	}

	if poolNum == len(p.baseline) {
		*b = make([]byte, n)
		return *b
	}
	*b = make([]byte, p.baseline[poolNum])
	*b = (*b)[:n]
	return *b
}

// Put adds given buffer to the pool.
func (p *BufferPool) Put(b []byte) {
	if p == nil {
		return
	}

	poolNum := p.poolNum(cap(b))

	atomic.AddUint32(&p.put, 1)
	// 放到系统自带的池子中 线程安全的
	p.pool[poolNum].Put(&b)
}

func (p *BufferPool) String() string {
	if p == nil {
		return "<nil>"
	}
	return fmt.Sprintf("BufferPool{B·%d G·%d P·%d <·%d =·%d >·%d M·%d}",
		p.baseline, p.get, p.put, p.less, p.equal, p.greater, p.miss)
}

// NewBufferPool creates a new initialized 'buffer pool'.
func NewBufferPool(baseline int) *BufferPool {
	if baseline <= 0 {
		panic("baseline can't be <= 0")
	}
	bufPool := &BufferPool{
		// 我不理解 这为什么要设置这三个基线
		// 所以 一般情况下 只用开辟baseline*8个字节的buffer就能供应商所有的人
		baseline: [...]int{baseline / 4, baseline / 2, baseline, baseline * 2, baseline * 4},
		pool: [6]sync.Pool{
			sync.Pool{
				New: func() interface{} { return new([]byte) },
			},
			sync.Pool{
				New: func() interface{} { return new([]byte) },
			},
			sync.Pool{
				New: func() interface{} { return new([]byte) },
			},
			sync.Pool{
				New: func() interface{} { return new([]byte) },
			},
			sync.Pool{
				New: func() interface{} { return new([]byte) },
			},
			sync.Pool{
				New: func() interface{} { return new([]byte) },
			},
		},
	}

	return bufPool
}
