package raft

import (
	"6.824/labgob"
	"bytes"
)

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.LastIncludedIndex || index >= rf.LastIncludedIndex+len(rf.Logs) {
		return
	}
	log := rf.Logs[index-rf.LastIncludedIndex]
	rf.Logs = rf.Logs[index-rf.LastIncludedIndex:]
	rf.LastIncludedIndex = log.Index
	rf.LastIncludedTerm = log.Term
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.CurrentTerm) != nil || e.Encode(rf.VotedFor) != nil || e.Encode(rf.Logs) != nil || e.Encode(rf.LastIncludedIndex) != nil || e.Encode(rf.LastIncludedTerm) != nil {
		panic("encode fail when persist raft state")
	}
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data, snapshot)
}

type InstallSnapshotArgs struct {
	Term              int
	LastIncludedIndex int
	LastIncludedTerm  int
	Snapshot          []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.Role != Candidate {
		rf.resetTimer()
	}
	reply.Term = rf.CurrentTerm
	if args.Term < rf.CurrentTerm {
		return
	}
	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.Role = Follower
		rf.VotedFor = -1
		rf.resetTimer()
		rf.persist()
	}
	if args.LastIncludedIndex <= rf.LastIncludedIndex || args.LastIncludedIndex <= rf.LastApplied {
		return
	}
	if args.LastIncludedIndex-rf.LastIncludedIndex < len(rf.Logs) && rf.Logs[args.LastIncludedIndex-rf.LastIncludedIndex].Term == rf.LastIncludedTerm {
		rf.Logs = rf.Logs[args.LastIncludedIndex-rf.LastIncludedIndex:]
	} else {
		rf.Logs = []*LogEntry{}
		rf.Logs = append(rf.Logs, &LogEntry{Index: args.LastIncludedIndex, Term: args.LastIncludedTerm})
	}
	rf.LastIncludedIndex = args.LastIncludedIndex
	rf.LastIncludedTerm = args.LastIncludedTerm
	rf.ApplyAgent <- ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      args.Snapshot,
		SnapshotIndex: args.LastIncludedIndex,
		SnapshotTerm:  args.LastIncludedTerm,
	}
	rf.LastApplied = rf.LastIncludedIndex
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.CurrentTerm) != nil || e.Encode(rf.VotedFor) != nil || e.Encode(rf.Logs) != nil || e.Encode(rf.LastIncludedIndex) != nil || e.Encode(rf.LastIncludedTerm) != nil {
		panic("encode fail when persist raft state")
	}
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data, args.Snapshot)
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	return rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
}

func (rf *Raft) sendInstallSnapshotToPeer(server int, args *InstallSnapshotArgs) {
	for !rf.killed() {
		if rf.CurrentTerm > args.Term || rf.Role != Leader {
			return
		}
		reply := InstallSnapshotReply{}
		ok := rf.sendInstallSnapshot(server, args, &reply)
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
		rf.MatchIndex[server] = args.LastIncludedIndex
		rf.NextIndex[server] = args.LastIncludedIndex + 1
		rf.Commit()
		rf.mu.Unlock()
		break
	}
}
