package raft

import "fmt"

// do checkpoint from app layer
func (rl *RaftLog) doSnapshot(index int, snapshot []byte) {
	idx := rl.idx(index)

	rl.snapLastIdx = index
	rl.snapLastTerm = rl.tailLog[idx].Term
	rl.snapshot = snapshot

	newLog := make([]LogEntry, 0, rl.size()-rl.snapLastIdx)
	newLog = append(newLog, LogEntry{Term: rl.snapLastTerm})
	newLog = append(newLog, rl.tailLog[idx+1:]...)
	rl.tailLog = newLog
}

// do checkpoint from Leader
func (rl *RaftLog) installSnapShot(index, term int, snapshot []byte) {

	rl.snapLastIdx = index
	rl.snapLastTerm = term
	rl.snapshot = snapshot

	newLog := make([]LogEntry, 0, 1)
	newLog = append(newLog, LogEntry{Term: rl.snapLastTerm})
	rl.tailLog = newLog
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int

	Snapshot []byte
}

func (args *InstallSnapshotArgs) String() string {
	return fmt.Sprintf("Leader: %d,T%d,Last: [%d]T%d",
		args.LeaderId, args.Term, args.LastIncludedIndex, args.LastIncludedTerm)
}

type InstallSnapshotReply struct {
	Term int
}

func (reply *InstallSnapshotReply) String() string {
	return fmt.Sprintf("T%d", reply.Term)
}

// Follower
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// For debug
	LOG(rf.me, rf.currentTerm, DDebug, "<- S%d, Receive snapshot, Args:%s", args.LeaderId, args.String())

	reply.Term = rf.currentTerm
	//align the term
	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DSnap, "<- S%d, Reject Snap, Higher Term: T%d > T%d", args.LeaderId, rf.currentTerm, args.Term)
		return
	}
	rf.becomeFollowerLocked(args.Term)

	// check whether it has a longer snapshot
	if rf.log.snapLastIdx >= args.LastIncludedIndex {
		LOG(rf.me, rf.currentTerm, DSnap, "<- S%d,Reject Snap, Longer Snap already have: [%d] > [%d]", args.LeaderId, rf.log.snapLastIdx, args.LastIncludedIndex)
		return
	}

	// install snapshot and persist and apply
	rf.log.installSnapShot(args.LastIncludedIndex, args.LastIncludedTerm, args.Snapshot)
	rf.persistLocked()
	rf.snapPending = true
	rf.applyCond.Signal()
}

// Leader
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) installToPeer(peer int, term int, args *InstallSnapshotArgs) {
	reply := &InstallSnapshotReply{}
	ok := rf.sendInstallSnapshot(peer, args, reply)

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

	// update match/next
	if args.LastIncludedIndex > rf.matchIndex[peer] {
		rf.matchIndex[peer] = args.LastIncludedIndex
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1
	}
}
