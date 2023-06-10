package shardctrler

//
// Shardctrler clerk.
//

import (
	"6.824/labrpc"
	"sync"
)
import "time"
import "crypto/rand"
import "math/big"

const replyTimeout = 50 * time.Millisecond

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	id        int64
	mu        sync.Mutex
	commandId int64
	leader    int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.id = nrand()
	ck.commandId = 1
	ck.leader = 0
	return ck
}

type rpcReply struct {
	succeed bool
	server  int
	config  Config
}

func (ck *Clerk) Query(num int) Config {
	// Your code here.
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := QueryArgs{Num: num, ClerkId: ck.id, CommandId: ck.commandId}
	ck.commandId++
	ch := make(chan rpcReply)
	for i := ck.leader; ; i = (i + 1) % len(ck.servers) {
		timer := time.NewTimer(replyTimeout)
		go func(server int) {
			reply := QueryReply{}
			ok := ck.servers[server].Call("ShardCtrler.Query", &args, &reply)
			if ok && !reply.WrongLeader && reply.Err == "" {
				ch <- rpcReply{succeed: true, server: server, config: reply.Config}
			} else {
				ch <- rpcReply{succeed: false}
			}
		}(i)
		select {
		case <-timer.C:
			continue
		case reply := <-ch:
			if reply.succeed {
				ck.leader = reply.server
				return reply.config
			}
			continue
		}
	}
	//for {
	//	// try each known server.
	//	for _, srv := range ck.servers {
	//		var reply QueryReply
	//		ok := srv.Call("ShardCtrler.Query", args, &reply)
	//		if ok && reply.WrongLeader == false {
	//			return reply.Config
	//		}
	//	}
	//	time.Sleep(100 * time.Millisecond)
	//}
}

func (ck *Clerk) Join(servers map[int][]string) {
	// Your code here.
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := JoinArgs{Servers: servers, ClerkId: ck.id, CommandId: ck.commandId}
	ck.commandId++
	ch := make(chan rpcReply)
	for i := ck.leader; ; i = (i + 1) % len(ck.servers) {
		timer := time.NewTimer(replyTimeout)
		go func(server int) {
			reply := JoinReply{}
			ok := ck.servers[server].Call("ShardCtrler.Join", &args, &reply)
			if ok && !reply.WrongLeader && reply.Err == "" {
				ch <- rpcReply{succeed: true, server: server}
			} else {
				ch <- rpcReply{succeed: false}
			}
		}(i)
		select {
		case <-timer.C:
			continue
		case reply := <-ch:
			if reply.succeed {
				ck.leader = reply.server
				return
			}
			continue
		}
	}
	//for {
	//	// try each known server.
	//	for _, srv := range ck.servers {
	//		var reply JoinReply
	//		ok := srv.Call("ShardCtrler.Join", args, &reply)
	//		if ok && reply.WrongLeader == false {
	//			return
	//		}
	//	}
	//	time.Sleep(100 * time.Millisecond)
	//}
}

func (ck *Clerk) Leave(gids []int) {
	// Your code here.
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := LeaveArgs{GIDs: gids, ClerkId: ck.id, CommandId: ck.commandId}
	ck.commandId++
	ch := make(chan rpcReply)
	for i := ck.leader; ; i = (i + 1) % len(ck.servers) {
		timer := time.NewTimer(replyTimeout)
		go func(server int) {
			reply := LeaveReply{}
			ok := ck.servers[server].Call("ShardCtrler.Leave", &args, &reply)
			if ok && !reply.WrongLeader && reply.Err == "" {
				ch <- rpcReply{succeed: true, server: server}
			} else {
				ch <- rpcReply{succeed: false}
			}
		}(i)
		select {
		case <-timer.C:
			continue
		case reply := <-ch:
			if reply.succeed {
				ck.leader = reply.server
				return
			}
			continue
		}
	}
	//for {
	//	// try each known server.
	//	for _, srv := range ck.servers {
	//		var reply LeaveReply
	//		ok := srv.Call("ShardCtrler.Leave", args, &reply)
	//		if ok && reply.WrongLeader == false {
	//			return
	//		}
	//	}
	//	time.Sleep(100 * time.Millisecond)
	//}
}

func (ck *Clerk) Move(shard int, gid int) {
	// Your code here.
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := MoveArgs{Shard: shard, GID: gid, ClerkId: ck.id, CommandId: ck.commandId}
	ck.commandId++
	ch := make(chan rpcReply)
	for i := ck.leader; ; i = (i + 1) % len(ck.servers) {
		timer := time.NewTimer(replyTimeout)
		go func(server int) {
			reply := MoveReply{}
			ok := ck.servers[server].Call("ShardCtrler.Move", &args, &reply)
			if ok && !reply.WrongLeader && reply.Err == "" {
				ch <- rpcReply{succeed: true, server: server}
			} else {
				ch <- rpcReply{succeed: false}
			}
		}(i)
		select {
		case <-timer.C:
			continue
		case reply := <-ch:
			if reply.succeed {
				ck.leader = reply.server
				return
			}
			continue
		}
	}
	//for {
	//	// try each known server.
	//	for _, srv := range ck.servers {
	//		var reply MoveReply
	//		ok := srv.Call("ShardCtrler.Move", args, &reply)
	//		if ok && reply.WrongLeader == false {
	//			return
	//		}
	//	}
	//	time.Sleep(100 * time.Millisecond)
	//}
}
