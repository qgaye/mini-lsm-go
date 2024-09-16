package main

type MiniLsm struct {
	inner *LsmStorageInner
}

func (r *MiniLsm) Get(key []byte) ([]byte, bool) {
	return r.inner.Get(key)
}

func (r *MiniLsm) Put(key []byte, value []byte) {
	r.inner.Put(key, value)
}

func (r *MiniLsm) Del(key []byte) {
	r.inner.Del(key)
}
