package raft

func (rf *Raft) Apply() {
	//fmt.Printf("[%v] lastApplied: %v, commitIndex: %v, lastIncludedIndex: %v, logsNum: %v, isleader: %v", rf.me, rf.LastApplied, rf.CommitIndex, rf.LastIncludedIndex, len(rf.Logs), rf.Role == Leader)
	if rf.LastApplied < rf.LastIncludedIndex {
		rf.LastApplied = rf.LastIncludedIndex
	}
	for rf.LastApplied < rf.CommitIndex {
		rf.LastApplied++
		rf.ApplyAgent <- ApplyMsg{
			CommandValid: true,
			Command:      rf.Logs[rf.LastApplied-rf.LastIncludedIndex].Command,
			CommandIndex: rf.Logs[rf.LastApplied-rf.LastIncludedIndex].Index,
		}
	}
	//fmt.Printf(" newLastApplied: %v\n", rf.LastApplied)
}

func (rf *Raft) tryApply() {
	for !rf.killed() {
		applyMsg := <-rf.ApplyAgent
		rf.ApplyCh <- applyMsg
	}
}
