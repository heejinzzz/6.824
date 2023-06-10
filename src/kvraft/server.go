package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"log"
	"sync"
	"sync/atomic"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type OpType int8

const (
	GET    OpType = 0
	PUT    OpType = 1
	APPEND OpType = 2
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClerkId   int64
	CommandId int64
	Type      OpType
	Key       string
	Value     string
}

type CommandMsg struct {
	execute bool
	value   string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	persister            *raft.Persister
	db                   *DB
	clerkCommandRecord   map[int64]int64
	clerkCommandInformer map[int64]map[int64]chan CommandMsg
	lastAppliedIndex     int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	if args.CommandId <= kv.clerkCommandRecord[args.ClerkId] {
		//fmt.Println("command", args.CommandId, "has been executed before command", kv.clerkCommandRecord[args.ClerkId])
		reply.Value = kv.db.Get(args.Key)
		kv.mu.Unlock()
		return
	}
	_, _, ok := kv.rf.Start(Op{ClerkId: args.ClerkId, CommandId: args.CommandId, Type: GET, Key: args.Key})
	if !ok {
		kv.mu.Unlock()
		reply.Err = "this server is not a leader"
		return
	}
	if kv.clerkCommandInformer[args.ClerkId] == nil {
		kv.clerkCommandInformer[args.ClerkId] = map[int64]chan CommandMsg{}
	}
	ch := make(chan CommandMsg)
	kv.clerkCommandInformer[args.ClerkId][args.CommandId] = ch
	kv.mu.Unlock()
	commandMsg := <-ch
	if !commandMsg.execute {
		reply.Err = "this command has been executed"
		return
	}
	reply.Err = ""
	reply.Value = commandMsg.value
	return
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	if args.CommandId <= kv.clerkCommandRecord[args.ClerkId] {
		//fmt.Println("command", args.CommandId, args.Op, args.Key, ":", args.Value, "has been executed before command", kv.clerkCommandRecord[args.ClerkId])
		kv.mu.Unlock()
		return
	}
	opType := PUT
	if args.Op == "Append" {
		opType = APPEND
	}
	_, _, ok := kv.rf.Start(Op{ClerkId: args.ClerkId, CommandId: args.CommandId, Type: opType, Key: args.Key, Value: args.Value})
	if !ok {
		kv.mu.Unlock()
		reply.Err = "this server is not a leader"
		return
	}
	if kv.clerkCommandInformer[args.ClerkId] == nil {
		kv.clerkCommandInformer[args.ClerkId] = map[int64]chan CommandMsg{}
	}
	ch := make(chan CommandMsg)
	kv.clerkCommandInformer[args.ClerkId][args.CommandId] = ch
	kv.mu.Unlock()
	commandMsg := <-ch
	if !commandMsg.execute {
		reply.Err = "this command has been executed"
		return
	}
	reply.Err = ""
	return
}

func (kv *KVServer) listenApplyCh() {
	for !kv.killed() {
		applyMsg := <-kv.applyCh
		kv.mu.Lock()
		if applyMsg.CommandValid {
			kv.lastAppliedIndex = applyMsg.CommandIndex
			op := applyMsg.Command.(Op)
			if op.CommandId <= kv.clerkCommandRecord[op.ClerkId] {
				if kv.clerkCommandInformer[op.ClerkId] != nil && kv.clerkCommandInformer[op.ClerkId][op.CommandId] != nil {
					//if kv.rf.Role == raft.Leader {
					//	kv.clerkCommandInformer[op.ClerkId][op.CommandId] <- CommandMsg{execute: false}
					//}
					kv.clerkCommandInformer[op.ClerkId][op.CommandId] <- CommandMsg{execute: false}
					kv.clerkCommandInformer[op.ClerkId][op.CommandId] = nil
					delete(kv.clerkCommandInformer[op.ClerkId], op.CommandId)
				}
				kv.mu.Unlock()
				continue
			}
			var value string
			switch op.Type {
			case GET:
				value = kv.db.Get(op.Key)
			case PUT:
				kv.db.Put(op.Key, op.Value)
			case APPEND:
				kv.db.Append(op.Key, op.Value)
				//fmt.Printf("[%v] succeed to execute log %v append key %v value: %v. Current value: %v\n", kv.me, applyMsg.CommandIndex, op.Key, op.Value, kv.db.Get(op.Key))
			}
			if kv.clerkCommandInformer[op.ClerkId] != nil && kv.clerkCommandInformer[op.ClerkId][op.CommandId] != nil {
				//if kv.rf.Role == raft.Leader {
				//	kv.clerkCommandInformer[op.ClerkId][op.CommandId] <- CommandMsg{execute: true, value: value}
				//}
				kv.clerkCommandInformer[op.ClerkId][op.CommandId] <- CommandMsg{execute: true, value: value}
				kv.clerkCommandInformer[op.ClerkId][op.CommandId] = nil
				delete(kv.clerkCommandInformer[op.ClerkId], op.CommandId)
			}
			kv.clerkCommandRecord[op.ClerkId] = op.CommandId
			kv.checkRaftStateSize()
		} else {
			//fmt.Printf("[%v] install snapshot with lastAppliedIndex %v\n", kv.me, applyMsg.SnapshotIndex)
			kv.LoadSnapshot(applyMsg.Snapshot)
		}
		kv.mu.Unlock()
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.persister = persister
	kv.db = NewDB()
	kv.clerkCommandRecord = map[int64]int64{}
	kv.clerkCommandInformer = map[int64]map[int64]chan CommandMsg{}

	kv.LoadSnapshot(persister.ReadSnapshot())

	go kv.listenApplyCh()

	//fmt.Printf("[%v] start with lastIncludedIndex: %v, logsNum: %v, data: %v, logs: %v\n", me, kv.rf.LastIncludedIndex, len(kv.rf.Logs), kv.db.data, kv.rf.Logs)
	return kv
}
