package shardctrler

import (
	"6.824/raft"
)
import "6.824/labrpc"
import "sync"
import "6.824/labgob"

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs              []Config // indexed by config num
	clerkCommandRecord   map[int64]int64
	clerkCommandInformer map[int64]map[int64]chan CommandMsg
}

type CommandMsg struct {
	err    Err
	config Config // valid when OpType == QUERY
}

type OpType int8

const (
	JOIN  OpType = 0
	LEAVE OpType = 1
	MOVE  OpType = 2
	QUERY OpType = 3
)

type Op struct {
	// Your data here.
	ClerkId   int64
	CommandId int64
	Type      OpType
	Servers   map[int][]string // valid when Type == JOIN
	GIDs      []int            // valid when Type == LEAVE
	Shared    int              // valid when Type == MOVE
	GID       int              // valid when Type == MOVE
	Num       int              // valid when Type == QUERY
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sc.mu.Lock()
	if args.CommandId <= sc.clerkCommandRecord[args.ClerkId] {
		sc.mu.Unlock()
		return
	}
	_, _, ok := sc.rf.Start(Op{ClerkId: args.ClerkId, CommandId: args.CommandId, Type: JOIN, Servers: args.Servers})
	if !ok {
		sc.mu.Unlock()
		reply.WrongLeader, reply.Err = true, "this server is not a leader"
		return
	}
	if sc.clerkCommandInformer[args.ClerkId] == nil {
		sc.clerkCommandInformer[args.ClerkId] = map[int64]chan CommandMsg{}
	}
	ch := make(chan CommandMsg)
	sc.clerkCommandInformer[args.ClerkId][args.CommandId] = ch
	sc.mu.Unlock()
	commandMsg := <-ch
	reply.Err = commandMsg.err
	return
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sc.mu.Lock()
	if args.CommandId <= sc.clerkCommandRecord[args.ClerkId] {
		sc.mu.Unlock()
		return
	}
	_, _, ok := sc.rf.Start(Op{ClerkId: args.ClerkId, CommandId: args.CommandId, Type: LEAVE, GIDs: args.GIDs})
	if !ok {
		sc.mu.Unlock()
		reply.WrongLeader, reply.Err = true, "this server is not a leader"
		return
	}
	if sc.clerkCommandInformer[args.ClerkId] == nil {
		sc.clerkCommandInformer[args.ClerkId] = map[int64]chan CommandMsg{}
	}
	ch := make(chan CommandMsg)
	sc.clerkCommandInformer[args.ClerkId][args.CommandId] = ch
	sc.mu.Unlock()
	commandMsg := <-ch
	reply.Err = commandMsg.err
	return
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sc.mu.Lock()
	if args.CommandId <= sc.clerkCommandRecord[args.ClerkId] {
		sc.mu.Unlock()
		return
	}
	_, _, ok := sc.rf.Start(Op{ClerkId: args.ClerkId, CommandId: args.CommandId, Type: MOVE, Shared: args.Shard, GID: args.GID})
	if !ok {
		sc.mu.Unlock()
		reply.WrongLeader, reply.Err = true, "this server is not a leader"
		return
	}
	if sc.clerkCommandInformer[args.ClerkId] == nil {
		sc.clerkCommandInformer[args.ClerkId] = map[int64]chan CommandMsg{}
	}
	ch := make(chan CommandMsg)
	sc.clerkCommandInformer[args.ClerkId][args.CommandId] = ch
	sc.mu.Unlock()
	commandMsg := <-ch
	reply.Err = commandMsg.err
	return
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sc.mu.Lock()
	if args.CommandId <= sc.clerkCommandRecord[args.ClerkId] {
		if args.Num == -1 || args.Num >= len(sc.configs) {
			reply.Config = sc.configs[len(sc.configs)-1]
		} else {
			reply.Config = sc.configs[args.Num]
		}
		sc.mu.Unlock()
		return
	}
	_, _, ok := sc.rf.Start(Op{ClerkId: args.ClerkId, CommandId: args.CommandId, Type: QUERY, Num: args.Num})
	if !ok {
		sc.mu.Unlock()
		reply.WrongLeader, reply.Err = true, "this server is not a leader"
		return
	}
	if sc.clerkCommandInformer[args.ClerkId] == nil {
		sc.clerkCommandInformer[args.ClerkId] = map[int64]chan CommandMsg{}
	}
	ch := make(chan CommandMsg)
	sc.clerkCommandInformer[args.ClerkId][args.CommandId] = ch
	sc.mu.Unlock()
	commandMsg := <-ch
	if commandMsg.err == "" {
		reply.Config = commandMsg.config
		return
	}
	reply.Err = commandMsg.err
	return
}

func (sc *ShardCtrler) listenApplyCh() {
	for {
		applyMsg := <-sc.applyCh
		if !applyMsg.CommandValid {
			continue
		}
		sc.mu.Lock()
		op := applyMsg.Command.(Op)
		if op.CommandId <= sc.clerkCommandRecord[op.ClerkId] {
			if sc.clerkCommandInformer[op.ClerkId] != nil && sc.clerkCommandInformer[op.ClerkId][op.CommandId] != nil {
				sc.clerkCommandInformer[op.ClerkId][op.CommandId] <- CommandMsg{err: "this command has been executed before"}
				sc.clerkCommandInformer[op.ClerkId][op.CommandId] = nil
				delete(sc.clerkCommandInformer[op.ClerkId], op.CommandId)
			}
			sc.mu.Unlock()
			continue
		}
		commandMsg := CommandMsg{}
		switch op.Type {
		case JOIN:
			sc.join(op.Servers)
		case LEAVE:
			sc.leave(op.GIDs)
		case MOVE:
			sc.move(op.Shared, op.GID)
		case QUERY:
			if op.Num == -1 || op.Num >= len(sc.configs) {
				commandMsg.config = sc.configs[len(sc.configs)-1]
			} else {
				commandMsg.config = sc.configs[op.Num]
			}
		}
		if sc.clerkCommandInformer[op.ClerkId] != nil && sc.clerkCommandInformer[op.ClerkId][op.CommandId] != nil {
			sc.clerkCommandInformer[op.ClerkId][op.CommandId] <- commandMsg
			sc.clerkCommandInformer[op.ClerkId][op.CommandId] = nil
			delete(sc.clerkCommandInformer[op.ClerkId], op.CommandId)
		}
		sc.clerkCommandRecord[op.ClerkId] = op.CommandId
		sc.mu.Unlock()
	}
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.clerkCommandRecord = map[int64]int64{}
	sc.clerkCommandInformer = map[int64]map[int64]chan CommandMsg{}

	go sc.listenApplyCh()

	return sc
}
