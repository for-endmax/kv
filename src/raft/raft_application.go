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

		snapPendingApply := rf.snapPending
		if !snapPendingApply {
			// make sure that the rf.log have all the entries
			start := rf.lastApplied + 1
			end := rf.commitIndex
			if end >= rf.log.size() {
				end = rf.log.size() - 1
			}
			for i := start; i <= end; i++ {
				entries = append(entries, rf.log.at(i))
			}
		}
		rf.mu.Unlock()

		if !snapPendingApply {
			for i, entry := range entries {
				rf.applyCh <- ApplyMsg{
					CommandValid: entry.CommandValid,
					Command:      entry.Command,
					CommandIndex: rf.lastApplied + 1 + i,
				}
			}
		} else {
			rf.applyCh <- ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.log.snapshot,
				SnapshotTerm:  rf.log.snapLastTerm,
				SnapshotIndex: rf.log.snapLastIdx,
			}
		}

		rf.mu.Lock()
		if !snapPendingApply {
			LOG(rf.me, rf.currentTerm, DApply, "Apply log for [%d, %d]", rf.lastApplied+1, rf.lastApplied+len(entries))
			rf.lastApplied += len(entries)
		} else {
			LOG(rf.me, rf.currentTerm, DSnap, "Apply snap for [0]T0, [%d]T%d", rf.log.snapLastIdx, rf.log.snapLastTerm)
			rf.lastApplied = rf.log.snapLastIdx
			if rf.commitIndex < rf.lastApplied {
				rf.commitIndex = rf.lastApplied
			}
			rf.snapPending = false
		}
		rf.mu.Unlock()
	}
}
