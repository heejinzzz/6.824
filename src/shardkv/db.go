package shardkv

import (
	"sync"
)

type DB struct {
	mu   sync.RWMutex
	data map[string]string
}

func NewDB() *DB {
	return &DB{data: map[string]string{}}
}

func (db *DB) Get(key string) string {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.data[key]
}

func (db *DB) Put(key string, value string) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.data[key] = value
}

func (db *DB) Append(key string, value string) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.data[key] += value
}

func (db *DB) PopShards(shards []bool) map[string]string {
	db.mu.Lock()
	defer db.mu.Unlock()
	ret := map[string]string{}
	for key, value := range db.data {
		shard := key2shard(key)
		if shards[shard] {
			ret[key] = value
			delete(db.data, key)
		}
	}
	return ret
}

func (db *DB) PushShards(data map[string]string) {
	db.mu.Lock()
	defer db.mu.Unlock()
	for key, value := range data {
		db.data[key] = value
	}
}
