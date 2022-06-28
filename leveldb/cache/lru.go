// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package cache

import (
	"sync"
	"unsafe"
)

type lruNode struct {
	n   *Node
	h   *Handle
	ban bool

	next, prev *lruNode
}

// 这个倒是不难 就是插入到at的后面
func (n *lruNode) insert(at *lruNode) {
	x := at.next
	at.next = n
	n.prev = at
	n.next = x
	x.prev = n
}

// 这个还考虑一手线程安全？自己删除自己就好
func (n *lruNode) remove() {
	if n.prev != nil {
		n.prev.next = n.next
		n.next.prev = n.prev
		n.prev = nil
		n.next = nil
	} else {
		panic("BUG: removing removed node")
	}
}

// TODO recent不太懂
type lru struct {
	mu       sync.Mutex
	capacity int
	used     int
	recent   lruNode
}

// 最近使用节点设置左右为自己 使用过的结点设置为0
func (r *lru) reset() {
	r.recent.next = &r.recent
	r.recent.prev = &r.recent
	r.used = 0
}

// 他是怎么知道自己是继承了接口的？ 所有的具有共性的方法定义在一起，任何其他类型只要实现了这些方法就是实现了这个接口。
func (r *lru) Capacity() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.capacity
}

// TODO 如果使用的不是大于就不用扩容？为啥是需要缩小啊
func (r *lru) SetCapacity(capacity int) {
	var evicted []*lruNode

	r.mu.Lock()
	// 这个尺寸不是个数 而是占用字节大小
	r.capacity = capacity
	// 如果缩容不够，就一直缩
	for r.used > r.capacity {
		rn := r.recent.prev
		if rn == nil {
			panic("BUG: invalid LRU used or capacity counter")
		}
		rn.remove()
		rn.n.CacheData = nil
		r.used -= rn.n.Size()
		// 懒回收
		evicted = append(evicted, rn)
	}
	r.mu.Unlock()

	for _, rn := range evicted {
		rn.h.Release()
	}
}

// 就是把这个节点 更新为最新用过的结点 相当于刷新一下
func (r *lru) Promote(n *Node) {
	var evicted []*lruNode

	r.mu.Lock()
	if n.CacheData == nil {
		// 这啥意思啊 我的大小<=cap? 这没错吗 不应该是n.size()+r.Size() <= r.cap？
		// 只要你是小于等于的就可以往里面加 如果超了 可以剔除一些过期的
		if n.Size() <= r.capacity {
			rn := &lruNode{n: n, h: n.GetHandle()}
			rn.insert(&r.recent)
			n.CacheData = unsafe.Pointer(rn)
			r.used += n.Size()

			// TODO 牺牲的是最近的前一个？这？
			for r.used > r.capacity {
				rn := r.recent.prev
				if rn == nil {
					panic("BUG: invalid LRU used or capacity counter")
				}
				rn.remove()
				rn.n.CacheData = nil
				r.used -= rn.n.Size()
				evicted = append(evicted, rn)
			}
		}
	} else {
		// 删了再插入 相当于刷新
		rn := (*lruNode)(n.CacheData)
		if !rn.ban {
			rn.remove()
			rn.insert(&r.recent)
		}
	}
	r.mu.Unlock()

	for _, rn := range evicted {
		rn.h.Release()
	}
}

// TODO ban的作用是？
func (r *lru) Ban(n *Node) {
	r.mu.Lock()
	if n.CacheData == nil {
		n.CacheData = unsafe.Pointer(&lruNode{n: n, ban: true})
	} else {
		rn := (*lruNode)(n.CacheData)
		// 如果已经ban了 不进行操作
		if !rn.ban {
			// 如果没ban 就删除缓存 然后设置ban为true
			rn.remove()
			rn.ban = true
			r.used -= rn.n.Size()
			r.mu.Unlock()
			// 这个时候已经不需要操作r了，所以就释放了锁 算是性能上的优化吧
			rn.h.Release()
			rn.h = nil
			return
		}
	}
	r.mu.Unlock()
}

// 放逐的意思
func (r *lru) Evict(n *Node) {
	r.mu.Lock()
	rn := (*lruNode)(n.CacheData)
	// 如果ban了 就不用管了
	if rn == nil || rn.ban {
		r.mu.Unlock()
		return
	}
	n.CacheData = nil
	r.mu.Unlock()

	rn.h.Release()
}

// 整个命名空间都干掉
func (r *lru) EvictNS(ns uint64) {
	var evicted []*lruNode

	r.mu.Lock()
	// TODO 这个recent感觉就是队尾的意思？
	for e := r.recent.prev; e != &r.recent; {
		rn := e
		e = e.prev
		if rn.n.NS() == ns {
			rn.remove()
			rn.n.CacheData = nil
			r.used -= rn.n.Size()
			evicted = append(evicted, rn)
		}
	}
	r.mu.Unlock()

	for _, rn := range evicted {
		rn.h.Release()
	}
}

// 全放逐了
func (r *lru) EvictAll() {
	r.mu.Lock()
	back := r.recent.prev
	for rn := back; rn != &r.recent; rn = rn.prev {
		rn.n.CacheData = nil
	}
	r.reset()
	r.mu.Unlock()

	for rn := back; rn != &r.recent; rn = rn.prev {
		rn.h.Release()
	}
}

// close什么都不做？
func (r *lru) Close() error {
	return nil
}

// NewLRU create a new LRU-cache.
func NewLRU(capacity int) Cacher {
	r := &lru{capacity: capacity}
	r.reset()
	return r
}
