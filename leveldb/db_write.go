// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"sync/atomic"
	"time"

	"github.com/syndtr/goleveldb/leveldb/memdb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

func (db *DB) writeJournal(batches []*Batch, seq uint64, sync bool) error {
	wr, err := db.journal.Next()
	if err != nil {
		return err
	}
	if err := writeBatchesWithHeader(wr, batches, seq); err != nil {
		return err
	}
	if err := db.journal.Flush(); err != nil {
		return err
	}
	if sync {
		return db.journalWriter.Sync()
	}
	return nil
}

func (db *DB) rotateMem(n int, wait bool) (mem *memDB, err error) {
	retryLimit := 3
retry:
	// Wait for pending memdb compaction.
	err = db.compTriggerWait(db.mcompCmdC)
	if err != nil {
		return
	}
	retryLimit--

	// Create new memdb and journal.
	mem, err = db.newMem(n)
	if err != nil {
		if err == errHasFrozenMem {
			if retryLimit <= 0 {
				panic("BUG: still has frozen memdb")
			}
			goto retry
		}
		return
	}

	// Schedule memdb compaction.
	if wait {
		err = db.compTriggerWait(db.mcompCmdC)
	} else {
		db.compTrigger(db.mcompCmdC)
	}
	return
}

// 刷memtable到第0层
func (db *DB) flush(n int) (mdb *memDB, mdbFree int, err error) {
	delayed := false
	// 默认8个文件就要慢下来
	slowdownTrigger := db.s.o.GetWriteL0SlowdownTrigger()
	// 默认12个文件就要暂停
	pauseTrigger := db.s.o.GetWriteL0PauseTrigger()
	// 刷盘操作
	flush := func() (retry bool) {
		// 获得memtable
		mdb = db.getEffectiveMem()
		if mdb == nil {
			err = ErrClosed
			return false
		}
		defer func() {
			// 如果要重试就把memtable放回到池子里
			if retry {
				mdb.decref()
				mdb = nil
			}
		}()
		// 第0层的File数量
		tLen := db.s.tLen(0)
		// memtable的空位数量
		mdbFree = mdb.Free()
		switch {
		case tLen >= slowdownTrigger && !delayed:
			delayed = true
			// 等一毫秒
			time.Sleep(time.Millisecond)
			// TODO db空闲的字节数>=n 就不用重试 是刷到db吗？
		case mdbFree >= n:
			return false
			// 如果到了暂停的时候
		case tLen >= pauseTrigger:
			delayed = true
			// Set the write paused flag explicitly.
			// 这个相当于加锁了
			atomic.StoreInt32(&db.inWritePaused, 1)
			// 这个地方会阻塞
			err = db.compTriggerWait(db.tcompCmdC)
			// Unset the write paused flag.
			atomic.StoreInt32(&db.inWritePaused, 0)
			if err != nil {
				return false
			}
		default:
			// Allow memdb to grow if it has no entry.
			// 如果长度为0 那么所有的都是空闲
			if mdb.Len() == 0 {
				mdbFree = n
			} else {
				// 否则要引用计数减一
				mdb.decref()
				// 压缩创建新的
				mdb, err = db.rotateMem(n, false)
				if err == nil {
					mdbFree = mdb.Free()
				} else {
					mdbFree = 0
				}
			}
			return false
		}
		return true
	}
	start := time.Now()
	// 死循环刷盘
	for flush() {
	}
	// 统计写延迟时间和次数
	if delayed {
		db.writeDelay += time.Since(start)
		db.writeDelayN++
		// 如果没delay但是写延迟次数大于1 写log然后清零
	} else if db.writeDelayN > 0 {
		db.logf("db@write was delayed N·%d T·%v", db.writeDelayN, db.writeDelay)
		// TODO 实在没看懂这在干嘛
		// TODO 2022.06.13
		atomic.AddInt32(&db.cWriteDelayN, int32(db.writeDelayN))
		atomic.AddInt64(&db.cWriteDelay, int64(db.writeDelay))
		db.writeDelay = 0
		db.writeDelayN = 0
	}
	return
}

type writeMerge struct {
	sync       bool
	batch      *Batch
	keyType    keyType
	key, value []byte
}

func (db *DB) unlockWrite(overflow bool, merged int, err error) {
	for i := 0; i < merged; i++ {
		db.writeAckC <- err
	}
	if overflow {
		// Pass lock to the next write (that failed to merge).
		db.writeMergedC <- false
	} else {
		// Release lock.
		<-db.writeLockC
	}
}

// ourBatch is batch that we can modify.
func (db *DB) writeLocked(batch, ourBatch *Batch, merge, sync bool) error {
	// Try to flush memdb. This method would also trying to throttle writes
	// if it is too fast and compaction cannot catch-up.
	mdb, mdbFree, err := db.flush(batch.internalLen)
	if err != nil {
		db.unlockWrite(false, 0, err)
		return err
	}
	defer mdb.decref()

	var (
		overflow bool
		merged   int
		batches  = []*Batch{batch}
	)

	if merge {
		// Merge limit.
		var mergeLimit int
		if batch.internalLen > 128<<10 {
			mergeLimit = (1 << 20) - batch.internalLen
		} else {
			mergeLimit = 128 << 10
		}
		mergeCap := mdbFree - batch.internalLen
		if mergeLimit > mergeCap {
			mergeLimit = mergeCap
		}

	merge:
		for mergeLimit > 0 {
			select {
			case incoming := <-db.writeMergeC:
				if incoming.batch != nil {
					// Merge batch.
					if incoming.batch.internalLen > mergeLimit {
						overflow = true
						break merge
					}
					batches = append(batches, incoming.batch)
					mergeLimit -= incoming.batch.internalLen
				} else {
					// Merge put.
					internalLen := len(incoming.key) + len(incoming.value) + 8
					if internalLen > mergeLimit {
						overflow = true
						break merge
					}
					if ourBatch == nil {
						ourBatch = db.batchPool.Get().(*Batch)
						ourBatch.Reset()
						batches = append(batches, ourBatch)
					}
					// We can use same batch since concurrent write doesn't
					// guarantee write order.
					ourBatch.appendRec(incoming.keyType, incoming.key, incoming.value)
					mergeLimit -= internalLen
				}
				sync = sync || incoming.sync
				merged++
				db.writeMergedC <- true

			default:
				break merge
			}
		}
	}

	// Release ourBatch if any.
	if ourBatch != nil {
		defer db.batchPool.Put(ourBatch)
	}

	// Seq number.
	seq := db.seq + 1

	// Write journal.
	if err := db.writeJournal(batches, seq, sync); err != nil {
		db.unlockWrite(overflow, merged, err)
		return err
	}

	// Put batches.
	for _, batch := range batches {
		if err := batch.putMem(seq, mdb.DB); err != nil {
			panic(err)
		}
		seq += uint64(batch.Len())
	}

	// Incr seq number.
	db.addSeq(uint64(batchesLen(batches)))

	// Rotate memdb if it's reach the threshold.
	if batch.internalLen >= mdbFree {
		db.rotateMem(0, false)
	}

	db.unlockWrite(overflow, merged, nil)
	return nil
}

// Write apply the given batch to the DB. The batch records will be applied
// sequentially. Write might be used concurrently, when used concurrently and
// batch is small enough, write will try to merge the batches. Set NoWriteMerge
// option to true to disable write merge.
//
// It is safe to modify the contents of the arguments after Write returns but
// not before. Write will not modify content of the batch.
func (db *DB) Write(batch *Batch, wo *opt.WriteOptions) error {
	if err := db.ok(); err != nil || batch == nil || batch.Len() == 0 {
		return err
	}

	// If the batch size is larger than write buffer, it may justified to write
	// using transaction instead. Using transaction the batch will be written
	// into tables directly, skipping the journaling.
	if batch.internalLen > db.s.o.GetWriteBuffer() && !db.s.o.GetDisableLargeBatchTransaction() {
		tr, err := db.OpenTransaction()
		if err != nil {
			return err
		}
		if err := tr.Write(batch, wo); err != nil {
			tr.Discard()
			return err
		}
		return tr.Commit()
	}

	merge := !wo.GetNoWriteMerge() && !db.s.o.GetNoWriteMerge()
	sync := wo.GetSync() && !db.s.o.GetNoSync()

	// Acquire write lock.
	if merge {
		select {
		case db.writeMergeC <- writeMerge{sync: sync, batch: batch}:
			if <-db.writeMergedC {
				// Write is merged.
				return <-db.writeAckC
			}
			// Write is not merged, the write lock is handed to us. Continue.
		case db.writeLockC <- struct{}{}:
			// Write lock acquired.
		case err := <-db.compPerErrC:
			// Compaction error.
			return err
		case <-db.closeC:
			// Closed
			return ErrClosed
		}
	} else {
		select {
		case db.writeLockC <- struct{}{}:
			// Write lock acquired.
		case err := <-db.compPerErrC:
			// Compaction error.
			return err
		case <-db.closeC:
			// Closed
			return ErrClosed
		}
	}

	return db.writeLocked(batch, nil, merge, sync)
}

func (db *DB) putRec(kt keyType, key, value []byte, wo *opt.WriteOptions) error {
	if err := db.ok(); err != nil {
		return err
	}

	// TODO db.session.cacheOptions是什么意思
	merge := !wo.GetNoWriteMerge() && !db.s.o.GetNoWriteMerge()
	sync := wo.GetSync() && !db.s.o.GetNoSync()

	// Acquire write lock.
	// 如果需要merge 先merge
	if merge {
		select {
		// 情况1 给writeMergeC发送merge消息
		// TODO 不是很懂 这种情况不是应该马上就进入下一行了吗，其他的会执行？
		case db.writeMergeC <- writeMerge{sync: sync, keyType: kt, key: key, value: value}:
			// 如果merge完事儿了 就返回
			// TODO 这里为什么不合并成一个就行？
			if <-db.writeMergedC {
				// Write is merged.
				return <-db.writeAckC
			}
			// Write is not merged, the write lock is handed to us. Continue.
			// 情况2 给写锁发送空消息 如果拿到了
		case db.writeLockC <- struct{}{}:
			// Write lock acquired.
			// 情况3 接受压缩失败的消息
		case err := <-db.compPerErrC:
			// Compaction error.
			return err
			// 情况4 收到通道关闭消息
		case <-db.closeC:
			// Closed
			return ErrClosed
		}
	} else {
		// 如果不需要merge
		select {
		// 情况1 拿写锁
		// 这是一个大小为1的channel
		// 妙啊 channel同一时刻只能有一个没有消费的 那这样就是把channel当做锁在用了
		// 要么是得到锁 要么是收到压缩失败消息 要么是收到关闭消息
		case db.writeLockC <- struct{}{}:
			// Write lock acquired.
			// 情况2 接受压缩失败的消息
		case err := <-db.compPerErrC:
			// Compaction error.
			return err
			// 情况3 收到通道关闭消息
		case <-db.closeC:
			// Closed
			return ErrClosed
		}
	}
	// TODO 为什么一定要拿写锁
	// 从池子里拿batch
	batch := db.batchPool.Get().(*Batch)
	// reset batch
	batch.Reset()
	// TODO 把记录加进batch（每个都这样如何batch？）
	batch.appendRec(kt, key, value)
	return db.writeLocked(batch, batch, merge, sync)
}

// Put sets the value for the given key. It overwrites any previous value
// for that key; a DB is not a multi-map. Write merge also applies for Put, see
// Write.
//
// It is safe to modify the contents of the arguments after Put returns but not
// before.
func (db *DB) Put(key, value []byte, wo *opt.WriteOptions) error {
	return db.putRec(keyTypeVal, key, value, wo)
}

// Delete deletes the value for the given key. Delete will not returns error if
// key doesn't exist. Write merge also applies for Delete, see Write.
//
// It is safe to modify the contents of the arguments after Delete returns but
// not before.
func (db *DB) Delete(key []byte, wo *opt.WriteOptions) error {
	return db.putRec(keyTypeDel, key, nil, wo)
}

func isMemOverlaps(icmp *iComparer, mem *memdb.DB, min, max []byte) bool {
	iter := mem.NewIterator(nil)
	defer iter.Release()
	return (max == nil || (iter.First() && icmp.uCompare(max, internalKey(iter.Key()).ukey()) >= 0)) &&
		(min == nil || (iter.Last() && icmp.uCompare(min, internalKey(iter.Key()).ukey()) <= 0))
}

// CompactRange compacts the underlying DB for the given key range.
// In particular, deleted and overwritten versions are discarded,
// and the data is rearranged to reduce the cost of operations
// needed to access the data. This operation should typically only
// be invoked by users who understand the underlying implementation.
//
// A nil Range.Start is treated as a key before all keys in the DB.
// And a nil Range.Limit is treated as a key after all keys in the DB.
// Therefore if both is nil then it will compact entire DB.
func (db *DB) CompactRange(r util.Range) error {
	if err := db.ok(); err != nil {
		return err
	}

	// Lock writer.
	select {
	case db.writeLockC <- struct{}{}:
	case err := <-db.compPerErrC:
		return err
	case <-db.closeC:
		return ErrClosed
	}

	// Check for overlaps in memdb.
	mdb := db.getEffectiveMem()
	if mdb == nil {
		return ErrClosed
	}
	defer mdb.decref()
	if isMemOverlaps(db.s.icmp, mdb.DB, r.Start, r.Limit) {
		// Memdb compaction.
		if _, err := db.rotateMem(0, false); err != nil {
			<-db.writeLockC
			return err
		}
		<-db.writeLockC
		if err := db.compTriggerWait(db.mcompCmdC); err != nil {
			return err
		}
	} else {
		<-db.writeLockC
	}

	// Table compaction.
	return db.compTriggerRange(db.tcompCmdC, -1, r.Start, r.Limit)
}

// SetReadOnly makes DB read-only. It will stay read-only until reopened.
func (db *DB) SetReadOnly() error {
	if err := db.ok(); err != nil {
		return err
	}

	// Lock writer.
	select {
	case db.writeLockC <- struct{}{}:
		db.compWriteLocking = true
	case err := <-db.compPerErrC:
		return err
	case <-db.closeC:
		return ErrClosed
	}

	// Set compaction read-only.
	select {
	case db.compErrSetC <- ErrReadOnly:
	case perr := <-db.compPerErrC:
		return perr
	case <-db.closeC:
		return ErrClosed
	}

	return nil
}
