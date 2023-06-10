package kvraft

import (
	"6.824/labrpc"
	"sync"
	"time"
)
import "crypto/rand"
import "math/big"

const replyTimeout = 50 * time.Millisecond

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
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
	// You'll have to add code here.
	ck.id = nrand()
	ck.commandId = 1
	ck.leader = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	//fmt.Println("get key", key)
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := GetArgs{ClerkId: ck.id, CommandId: ck.commandId, Key: key}
	ck.commandId++
	type rpcReply struct {
		succeed bool
		value   string
	}
	ch := make(chan rpcReply)
	for i := ck.leader; ; i = (i + 1) % len(ck.servers) {
		timer := time.NewTimer(replyTimeout)
		go func(server int) {
			reply := GetReply{}
			ok := ck.servers[server].Call("KVServer.Get", &args, &reply)
			if ok && reply.Err == "" {
				//fmt.Printf("[%v] ", i)
				ch <- rpcReply{succeed: true, value: reply.Value}
			} else {
				ch <- rpcReply{succeed: false}
			}
		}(i)
		select {
		case <-timer.C:
			continue
		case reply := <-ch:
			if reply.succeed {
				ck.leader = i
				//fmt.Println("complete get key", key, ":", reply.value)
				return reply.value
			}
			continue
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	//fmt.Println(op, "key", key, ":", value)
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := PutAppendArgs{ClerkId: ck.id, CommandId: ck.commandId, Key: key, Value: value, Op: op}
	ck.commandId++
	ch := make(chan bool)
	for i := ck.leader; ; i = (i + 1) % len(ck.servers) {
		timer := time.NewTimer(replyTimeout)
		go func(server int) {
			reply := PutAppendReply{}
			ok := ck.servers[server].Call("KVServer.PutAppend", &args, &reply)
			//ch <- ok && reply.Err == ""
			if ok && reply.Err == "" {
				//fmt.Printf("[%v] ", i)
				ch <- true
			} else {
				ch <- false
			}
		}(i)
		select {
		case <-timer.C:
			continue
		case ok := <-ch:
			if ok {
				ck.leader = i
				//fmt.Println("complete", op, "key", key, ":", value)
				return
			}
			continue
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
