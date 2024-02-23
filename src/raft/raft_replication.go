package raft

import (
	"fmt"
	"sort"
	"time"
)

type LogEntry struct {
	Term         int
	CommandValid bool
	Command      interface{}
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry

	LeaderCommit int
}

func (args *AppendEntriesArgs) String() string {
	return fmt.Sprintf("Leader: %d,T%d,Prev: [%d]T%d, Log: (%d,%d],CommitIdx: %d",
		args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.PrevLogIndex, args.PrevLogIndex+len(args.Entries), args.LeaderCommit)
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	//fields for optimization logs replication
	ConflictTerm  int
	ConflictIndex int
}

func (reply *AppendEntriesReply) String() string {
	return fmt.Sprintf("T%d, Success: %v, ConflictTerm: %d, ConflictIndex: %d ", reply.Term, reply.Success, reply.ConflictTerm, reply.ConflictIndex)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// For debug
	LOG(rf.me, rf.currentTerm, DDebug, "<- S%d, Receive log, Args:%s", args.LeaderId, args.String())

	reply.Term = rf.currentTerm
	reply.Success = false

	// align terms
	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject log, Higher term, T%d < T%d", args.LeaderId, args.Term, rf.currentTerm)
		return
	}
	// args.Term >= rf.currentTerm
	rf.becomeFollowerLocked(args.Term)

	// initialize electionTimer whether we accept logs
	defer func() {
		rf.resetElectionTimerLocked()
		if !reply.Success {
			LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Follower Conflict: [%d]T%d", args.LeaderId, reply.ConflictIndex, reply.ConflictTerm)
			LOG(rf.me, rf.currentTerm, DDebug, "<- S%d, Follower Log=%v", args.LeaderId, rf.log.String())
		}
	}()

	// return failure if the previous log not matched
	if args.PrevLogIndex >= rf.log.size() { // local logs is too short
		reply.ConflictTerm = InvalidTerm
		reply.ConflictIndex = rf.log.size()

		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject Log, Follower log too short, Len:%d <= Prev:%d", args.LeaderId, rf.log.size(), args.PrevLogIndex)
		return
	}
	if rf.log.at(args.PrevLogIndex).Term != args.PrevLogTerm {
		reply.ConflictTerm = rf.log.at(args.PrevLogIndex).Term
		reply.ConflictIndex = rf.log.firstFor(reply.ConflictTerm)

		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject Log, Prev log not match, [%d]: T%d != T%d", args.LeaderId, args.PrevLogIndex, rf.log.at(args.PrevLogIndex).Term, args.PrevLogTerm)
		return
	}

	// match success,append the leader logs to local
	rf.log.appendFrom(args.PrevLogIndex, args.Entries)
	rf.persistLocked()
	LOG(rf.me, rf.currentTerm, DLog2, "Follower append logs: (%d, %d]", args.PrevLogIndex, args.PrevLogIndex+len(args.Entries))
	reply.Success = true

	// update the commit index if needed and indicate the apply loop to apply
	if args.LeaderCommit > rf.commitIndex {
		LOG(rf.me, rf.currentTerm, DApply, "Follower update the commit index %d->%d", rf.commitIndex, args.LeaderCommit)
		rf.commitIndex = args.LeaderCommit
		if rf.commitIndex >= rf.log.size() {
			rf.commitIndex = rf.log.size() - 1
		}
		rf.applyCond.Signal()
	}

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) getMajorityIndexLocked() int {
	tmpIndexes := make([]int, len(rf.peers))
	copy(tmpIndexes, rf.matchIndex)
	sort.Ints(tmpIndexes)
	majorityIdx := (len(rf.peers) - 1) / 2
	LOG(rf.me, rf.currentTerm, DDebug, "Match index after sort: %v, majority[%d]=%d", tmpIndexes, majorityIdx, tmpIndexes[majorityIdx])
	return tmpIndexes[majorityIdx]
}
func MinInt(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

// only valid in the given term
func (rf *Raft) startReplication(term int) bool {
	replicateToPeer := func(peer int, args *AppendEntriesArgs) {
		reply := &AppendEntriesReply{}
		ok := rf.sendAppendEntries(peer, args, reply)

		rf.mu.Lock()
		defer rf.mu.Unlock()
		if !ok {
			LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Lost or error", peer)
			return
		}
		LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, Replicate log, Reply: %s", peer, reply.String())

		// align terms
		if reply.Term > rf.currentTerm {
			rf.becomeFollowerLocked(reply.Term)
			return
		}

		// whether context lost
		if rf.contextLostLocked(Leader, args.Term) {
			LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Context lost, T%d:Leader->T%d:%s", peer, args.Term, rf.currentTerm, rf.role)
			return
		}

		// success is false, probe
		if !reply.Success {
			prevNext := rf.nextIndex[peer]
			if reply.ConflictTerm == InvalidTerm {
				rf.nextIndex[peer] = reply.ConflictIndex
			} else {
				lastTermIndex := rf.log.lastFor(reply.ConflictTerm)
				if lastTermIndex != InvalidIndex {
					rf.nextIndex[peer] = lastTermIndex + 1
				} else {
					rf.nextIndex[peer] = reply.ConflictIndex
				}
			}
			rf.nextIndex[peer] = MinInt(prevNext, rf.nextIndex[peer]-1)

			// some of the replications are delayed, Leader may receive the same reply several times
			// in this situation , nextIndex[peer] may reduce to 0
			if rf.nextIndex[peer] == 0 {
				rf.nextIndex[peer] = 1
			}
			LOG(rf.me, rf.currentTerm, DLog, "-> S%d,Log not matched in Prev [%d]T%d, Update next Prev=[%d]T%d",
				peer, args.PrevLogIndex, args.PrevLogTerm, rf.nextIndex[peer]-1, rf.log.at(rf.nextIndex[peer]-1).Term)
			LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, Leader Log=%v", peer, rf.log.String())
			return
		}

		// success，update the match/next index if log appended successfully
		rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1

		majorityMatched := rf.getMajorityIndexLocked()
		if majorityMatched > rf.commitIndex && rf.log.at(majorityMatched).Term == rf.currentTerm { // important! can only commit log of current term
			LOG(rf.me, rf.currentTerm, DApply, "Leader update the commit index %d->%d", rf.commitIndex, majorityMatched)
			rf.commitIndex = majorityMatched
			rf.applyCond.Signal()
		}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.contextLostLocked(Leader, term) {
		LOG(rf.me, rf.currentTerm, DLog, "Lost Leader to %s(T%d)", rf.role, rf.currentTerm)
		return false
	}
	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			rf.matchIndex[peer] = rf.log.size() - 1
			rf.nextIndex[peer] = rf.log.size()
			continue
		}
		prevIdx := rf.nextIndex[peer] - 1
		prevTerm := rf.log.at(prevIdx).Term
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.votedFor,
			PrevLogIndex: prevIdx,
			PrevLogTerm:  prevTerm,
			Entries:      rf.log.tail(prevIdx),
			LeaderCommit: rf.commitIndex,
		}
		LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, Send log, Args:%s", peer, args.String())
		go replicateToPeer(peer, args)
	}
	return true
}

// could only replicate in the given term
func (rf *Raft) replicationTicker(term int) {
	for rf.killed() == false {

		ok := rf.startReplication(term)
		if !ok {
			break
		}
		time.Sleep(replicationInterval)
	}
}
