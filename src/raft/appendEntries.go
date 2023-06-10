package raft

import (
	"time"
)

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer func() {
		rf.persist()
		rf.mu.Unlock()
	}()
	if rf.Role != Candidate {
		rf.resetTimer()
	}
	reply.Term = rf.CurrentTerm
	if args.Term < rf.CurrentTerm {
		reply.Success = false
		return
	}
	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.Role = Follower
		rf.VotedFor = -1
		rf.resetTimer()
	}
	if args.PrevLogIndex >= rf.LastIncludedIndex+len(rf.Logs) {
		reply.Success = false
		return
	}
	if args.PrevLogIndex >= rf.LastIncludedIndex {
		log := rf.Logs[args.PrevLogIndex-rf.LastIncludedIndex]
		if log.Term != args.PrevLogTerm {
			reply.Success = false
			return
		}
	}
	ptr := 0
	if args.PrevLogIndex < rf.LastIncludedIndex {
		ptr = rf.LastIncludedIndex - args.PrevLogIndex - 1
	}
	for ptr < len(args.Entries) {
		entry := args.Entries[ptr]
		if entry.Index >= rf.LastIncludedIndex+len(rf.Logs) {
			break
		}
		if rf.Logs[entry.Index-rf.LastIncludedIndex].Term != entry.Term {
			rf.Logs = rf.Logs[:entry.Index-rf.LastIncludedIndex]
			break
		}
		ptr++
	}
	if ptr < len(args.Entries) {
		rf.Logs = append(rf.Logs, args.Entries[ptr:]...)
	}
	if args.LeaderCommit > rf.CommitIndex {
		if args.LeaderCommit < rf.Logs[len(rf.Logs)-1].Index {
			rf.CommitIndex = args.LeaderCommit
		} else {
			rf.CommitIndex = rf.Logs[len(rf.Logs)-1].Index
		}
		rf.Apply()
	}
	reply.Success = true
	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

func (rf *Raft) sendAppendEntriesToPeer(server int, args *AppendEntriesArgs) {
	for !rf.killed() {
		if args.Term < rf.CurrentTerm || rf.Role != Leader {
			return
		}
		reply := AppendEntriesReply{}
		ok := rf.sendAppendEntries(server, args, &reply)
		if !ok {
			continue
		}
		rf.mu.Lock()
		if reply.Term > rf.CurrentTerm {
			rf.CurrentTerm = reply.Term
			rf.Role = Follower
			rf.VotedFor = -1
			rf.resetTimer()
			rf.persist()
		}
		if args.Term < rf.CurrentTerm || rf.Role != Leader {
			rf.mu.Unlock()
			return
		}
		if reply.Success {
			var matchIndex int
			if len(args.Entries) > 0 {
				matchIndex = args.Entries[len(args.Entries)-1].Index
			} else {
				matchIndex = args.PrevLogIndex
			}
			rf.MatchIndex[server] = matchIndex
			rf.NextIndex[server] = matchIndex + 1
			rf.Commit()
			rf.mu.Unlock()
			break
		} else if rf.NextIndex[server] > 1 {
			rf.NextIndex[server]--
			if rf.NextIndex[server]-1 < rf.LastIncludedIndex {
				installSnapshotArgs := InstallSnapshotArgs{
					Term:              rf.CurrentTerm,
					LastIncludedIndex: rf.LastIncludedIndex,
					LastIncludedTerm:  rf.LastIncludedTerm,
					Snapshot:          rf.persister.ReadSnapshot(),
				}
				go rf.sendInstallSnapshotToPeer(server, &installSnapshotArgs)
				rf.mu.Unlock()
				return
			}
			prevLog := rf.Logs[rf.NextIndex[server]-rf.LastIncludedIndex-1]
			args.PrevLogIndex, args.PrevLogTerm = prevLog.Index, prevLog.Term
			args.Entries = rf.Logs[rf.NextIndex[server]-rf.LastIncludedIndex:]
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) broadcastAppendEntries() {
	for !rf.killed() {
		if rf.Role != Leader {
			continue
		}
		rf.mu.Lock()
		if rf.Role != Leader {
			rf.mu.Unlock()
			continue
		}
		term, leaderId, leaderCommit := rf.CurrentTerm, rf.me, rf.CommitIndex
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			if rf.NextIndex[i]-1 < rf.LastIncludedIndex {
				args := InstallSnapshotArgs{
					Term:              term,
					LastIncludedIndex: rf.LastIncludedIndex,
					LastIncludedTerm:  rf.LastIncludedTerm,
					Snapshot:          rf.persister.ReadSnapshot(),
				}
				go rf.sendInstallSnapshotToPeer(i, &args)
			} else {
				prevLog := rf.Logs[rf.NextIndex[i]-rf.LastIncludedIndex-1]
				prevLogIndex, prevLogTerm := prevLog.Index, prevLog.Term
				entries := rf.Logs[rf.NextIndex[i]-rf.LastIncludedIndex:]
				args := AppendEntriesArgs{
					Term:         term,
					LeaderId:     leaderId,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					Entries:      entries,
					LeaderCommit: leaderCommit,
				}
				go rf.sendAppendEntriesToPeer(i, &args)
			}
		}
		rf.mu.Unlock()
		time.Sleep(appendEntryInterval)
	}
}

func (rf *Raft) broadcastAppendEntriesOnce() {
	rf.mu.Lock()
	if rf.Role != Leader {
		rf.mu.Unlock()
		return
	}
	term, leaderId, leaderCommit := rf.CurrentTerm, rf.me, rf.CommitIndex
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		if rf.NextIndex[i]-1 < rf.LastIncludedIndex {
			args := InstallSnapshotArgs{
				Term:              term,
				LastIncludedIndex: rf.LastIncludedIndex,
				LastIncludedTerm:  rf.LastIncludedTerm,
				Snapshot:          rf.persister.ReadSnapshot(),
			}
			go rf.sendInstallSnapshotToPeer(i, &args)
		} else {
			prevLog := rf.Logs[rf.NextIndex[i]-rf.LastIncludedIndex-1]
			prevLogIndex, prevLogTerm := prevLog.Index, prevLog.Term
			entries := rf.Logs[rf.NextIndex[i]-rf.LastIncludedIndex:]
			args := AppendEntriesArgs{
				Term:         term,
				LeaderId:     leaderId,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: leaderCommit,
			}
			go rf.sendAppendEntriesToPeer(i, &args)
		}
	}
	rf.mu.Unlock()
}
