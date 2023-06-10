package raft

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

func (rf *Raft) resetTimer() {
	rand.Seed(time.Now().UnixNano() % rand.Int63() * int64(rf.me+1))
	rf.Timer.Reset(time.Duration(electionTimeoutMinMilliseconds+rand.Intn(electionTimeoutMaxMilliseconds-electionTimeoutMinMilliseconds)) * time.Millisecond)
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		<-rf.Timer.C
		rf.mu.Lock()
		if rf.Role != Follower {
			rf.resetTimer()
			rf.mu.Unlock()
			continue
		}
		rf.Role = Candidate
		rf.CurrentTerm++
		rf.VotedFor = rf.me
		rf.persist()
		lastLog := rf.Logs[len(rf.Logs)-1]
		go rf.elect(rf.CurrentTerm, lastLog.Index, lastLog.Term)
		rf.mu.Unlock()
	}
}

func (rf *Raft) elect(term int, lastLogIndex int, lastLogTerm int) {
	count := int32(1)
	wg := sync.WaitGroup{}
	wg.Add(len(rf.peers) - 1)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(serverId int) {
			defer wg.Done()
			ch := make(chan struct{})
			timer := time.NewTimer(rpcTimeout)
			go func() {
				args := RequestVoteArgs{
					Term:         term,
					CandidateId:  rf.me,
					LastLogIndex: lastLogIndex,
					LastLogTerm:  lastLogTerm,
				}
				reply := RequestVoteReply{}
				ok := rf.sendRequestVote(serverId, &args, &reply)
				if ok {
					if reply.VoteGranted {
						atomic.AddInt32(&count, 1)
					}
					rf.mu.Lock()
					if reply.Term > rf.CurrentTerm {
						rf.CurrentTerm = reply.Term
						rf.Role = Follower
						rf.VotedFor = -1
						rf.resetTimer()
					}
					rf.persist()
					rf.mu.Unlock()
				}
				ch <- struct{}{}
			}()
			select {
			case <-timer.C:
				return
			case <-ch:
				return
			}
		}(i)
	}
	wg.Wait()
	rf.tryBecomeLeader(term, int(count))
}

func (rf *Raft) tryBecomeLeader(term int, count int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.CurrentTerm > term || rf.Role != Candidate {
		//fmt.Printf("[%v %v] term change: %v -> %v\n", rf.CurrentTerm, rf.me, term, rf.CurrentTerm)
		return
	}
	if count <= len(rf.peers)/2 {
		//fmt.Printf("[%v %v] elect fail: %v/%v\n", rf.CurrentTerm, rf.me, count, len(rf.peers))
		rf.Role = Follower
		rf.resetTimer()
		return
	}
	//fmt.Printf("[%v %v] elect succeed of term %v: %v/%v\n", rf.CurrentTerm, rf.me, term, count, len(rf.peers))
	rf.Role = Leader
	for i := 0; i < len(rf.peers); i++ {
		rf.NextIndex[i] = rf.Logs[len(rf.Logs)-1].Index + 1
		rf.MatchIndex[i] = 0
	}
	rf.MatchIndex[rf.me] = rf.Logs[len(rf.Logs)-1].Index
	rf.resetTimer()
}
