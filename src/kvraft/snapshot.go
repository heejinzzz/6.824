package kvraft

import (
	"6.824/labgob"
	"bytes"
)

func (kv *KVServer) Snapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	kv.db.mu.RLock()
	defer kv.db.mu.RUnlock()
	if e.Encode(kv.clerkCommandRecord) != nil || e.Encode(kv.db.data) != nil || e.Encode(kv.lastAppliedIndex) != nil {
		panic("fail to encode snapshot of KVServer")
	}
	//fmt.Printf("[%v] snapshot with lastAppliedIndex: %v, data: %v\n", kv.me, kv.lastAppliedIndex, kv.db.data)
	return w.Bytes()
}

func (kv *KVServer) LoadSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) == 0 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var clerkCommandRecord map[int64]int64
	var data map[string]string
	var lastAppliedIndex int
	if d.Decode(&clerkCommandRecord) != nil || d.Decode(&data) != nil || d.Decode(&lastAppliedIndex) != nil {
		panic("fail to decode data from snapshot")
	}
	kv.db.mu.Lock()
	defer kv.db.mu.Unlock()
	kv.clerkCommandRecord = clerkCommandRecord
	kv.db.data = data
	kv.lastAppliedIndex = lastAppliedIndex
	//for key := range kv.clerkCommandInformer {
	//	kv.clerkCommandInformer[key] = nil
	//	delete(kv.clerkCommandInformer, key)
	//}
}

func (kv *KVServer) checkRaftStateSize() {
	if kv.maxraftstate == -1 || kv.persister.RaftStateSize() < kv.maxraftstate {
		return
	}
	snapshot := kv.Snapshot()
	kv.rf.Snapshot(kv.lastAppliedIndex, snapshot)
}
