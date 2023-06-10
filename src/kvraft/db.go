package kvraft

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
