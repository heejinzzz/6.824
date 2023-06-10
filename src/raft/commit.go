package raft

func (rf *Raft) Commit() {
	//fmt.Printf("[%v] current matchIndex: %v, current commitIndex: %v\n", rf.me, rf.MatchIndex, rf.CommitIndex)
	for i := len(rf.Logs) - 1; rf.Logs[i].Index > rf.CommitIndex && rf.Logs[i].Term == rf.CurrentTerm; i-- {
		count := 0
		for _, index := range rf.MatchIndex {
			if index >= rf.Logs[i].Index {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			rf.CommitIndex = rf.Logs[i].Index
			rf.Apply()
		}
	}
}
