package raft

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part PartD you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For PartD:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

func (rf *Raft) applicationTicker() {
	for !rf.killed() {
		rf.mu.Lock()
		rf.applyCond.Wait()

		entries := make([]LogEntry, 0)
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			entries = append(entries, rf.log.at(i))
		}
		rf.mu.Unlock()
		for i, entry := range entries {
			rf.applyCh <- ApplyMsg{
				CommandValid: entry.CommandValid,
				Command:      entry.Command,
				CommandIndex: rf.lastApplied + 1 + i,
			}
		}
		rf.mu.Lock()
		LOG(rf.me, rf.currentTerm, DApply, "Apply log for [%d, %d]", rf.lastApplied+1, rf.lastApplied+len(entries))
		rf.lastApplied += len(entries)
		rf.mu.Unlock()
	}
}
