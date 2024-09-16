package mem_table

import (
	"github.com/bytedance/gopkg/collection/skipmap"
	"sync/atomic"
)

type MemTableIterator interface {
}

type MemTable struct {
	Id               uint64
	Map              *skipmap.StringMap
	_approximateSize atomic.Uint64
}

func Create(id uint64) *MemTable {
	return &MemTable{
		Id:               id,
		Map:              skipmap.NewString(),
		_approximateSize: atomic.Uint64{},
	}
}

func (r *MemTable) Get(key []byte) ([]byte, bool) {
	val, ok := r.Map.Load(string(key))
	if !ok {
		return nil, false
	}
	if val == nil {
		return nil, true
	} else {
		return val.([]byte), true
	}
}

func (r *MemTable) Put(key []byte, value []byte) {
	estimatedSize := uint64(len(key) + len(value))
	r.Map.Store(string(key), value)
	r._approximateSize.Add(estimatedSize)
}

func (r *MemTable) Scan(lower []byte, upper []byte) MemTableIterator {
	return nil
}

func (r *MemTable) ApproximateSize() uint64 {
	return r._approximateSize.Load()
}
