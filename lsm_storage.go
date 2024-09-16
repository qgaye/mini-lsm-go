package main

import (
	"fmt"
	"mini-lsm-go/mem_table"
	"sync"
	"sync/atomic"
)

type Op = int

const (
	Op_Del Op = 1
	Op_Put Op = 2
)

type OpRecord struct {
	op    Op
	key   []byte
	value []byte
}

type LsmStorageState struct {
	sync.RWMutex
	memTable     *mem_table.MemTable
	immMemTables []*mem_table.MemTable
}

type LsmStorageInner struct {
	state      *LsmStorageState
	// lock ensure only one thread modify LsmStorageState, and rwLock in LsmStorageState to make when one thread is modifing LsmStorageState, other read thread can get kv, so when we need modify LsmStorageState, do `lock.Lock() and rwLock.WLock()`` 
	lock       sync.Mutex
	_nextSSTId atomic.Uint64
	options    *LsmStorageOptions
}

func (r *LsmStorageInner) Get(key []byte) ([]byte, bool) {
	r.state.RLock()
	defer r.state.RUnlock()

	val, ok := r.state.memTable.Get(key)
	if ok {
		return val, true
	}

	for _, memTable := range r.state.immMemTables {
		val, ok := memTable.Get(key)
		if ok {
			return val, true
		}
	}

	return nil, false
}

func (r *LsmStorageInner) Put(key []byte, value []byte) {
	r.writeBatch([]OpRecord{
		{
			op:    Op_Put,
			key:   key,
			value: value,
		},
	})
}

func (r *LsmStorageInner) Del(key []byte) {
	r.writeBatch([]OpRecord{
		{
			op:    Op_Del,
			key:   key,
			value: nil,
		},
	})
}

func (r *LsmStorageInner) writeBatch(records []OpRecord) {
	for _, record := range records {
		switch record.op {
		case Op_Put:
			r.state.RLock()
			r.state.memTable.Put(record.key, record.value)
			r.state.RUnlock()
		case Op_Del:
			r.state.RLock()
			r.state.memTable.Put(record.key, nil)
			r.state.RUnlock()
		default:
			panic(fmt.Sprintf("unsupport op %d", record.op))
		}
		r.tryFreeze()
	}
}

func (r *LsmStorageInner) forceFreezeMemtable() {
	memtableId := r.nextSSTId()
	r.state.Lock()
	defer r.state.Unlock()

	oldMemTable := r.state.memTable
	r.state.immMemTables = append([]*mem_table.MemTable{oldMemTable}, r.state.immMemTables...)
	r.state.memTable = mem_table.Create(memtableId)
}

func (r *LsmStorageInner) tryFreeze() {
	if r.state.memTable.ApproximateSize() >= r.options.TargetSSTSize {
		r.lock.Lock()
		defer r.lock.Unlock()

		// double check in concurrency
		if r.state.memTable.ApproximateSize() >= r.options.TargetSSTSize {
			r.forceFreezeMemtable()
		}
	}
}

func (r *LsmStorageInner) nextSSTId() uint64 {
	return r._nextSSTId.Add(1)
}

