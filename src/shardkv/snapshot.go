package shardkv

import (
	"6.824/labgob"
	"bytes"
)

func (kv *ShardKV) Snapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	kv.db.mu.RLock()
	defer kv.db.mu.RUnlock()
	if e.Encode(kv.clerkCommandRecord) != nil || e.Encode(kv.db.data) != nil || e.Encode(kv.lastAppliedIndex) != nil || e.Encode(kv.clerkId) != nil || e.Encode(kv.commandId) != nil || e.Encode(kv.ownShard) != nil {
		panic("fail to encode snapshot of KVServer")
	}
	return w.Bytes()
}

func (kv *ShardKV) LoadSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) == 0 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var clerkCommandRecord map[int64]int64
	var data map[string]string
	var lastAppliedIndex int
	var clerkId int64
	var commandId int64
	var ownShard []bool
	if d.Decode(&clerkCommandRecord) != nil || d.Decode(&data) != nil || d.Decode(&lastAppliedIndex) != nil || d.Decode(&clerkId) != nil || d.Decode(&commandId) != nil || d.Decode(&ownShard) != nil {
		panic("fail to decode data from snapshot")
	}
	kv.db.mu.Lock()
	defer kv.db.mu.Unlock()
	kv.clerkCommandRecord = clerkCommandRecord
	kv.db.data = data
	kv.lastAppliedIndex = lastAppliedIndex
	kv.clerkId = clerkId
	kv.commandId = commandId
	kv.ownShard = ownShard
}

func (kv *ShardKV) CheckRaftStateSize() {
	if kv.maxraftstate == -1 || kv.persister.RaftStateSize() < kv.maxraftstate {
		return
	}
	snapshot := kv.Snapshot()
	kv.rf.Snapshot(kv.lastAppliedIndex, snapshot)
}
