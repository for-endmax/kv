package raft

type Role string

const (
	Follower  Role = "Follower"
	Candidate Role = "Candidate"
	Leader    Role = "Leader"
)

func (rf *Raft) becomeFollowerLocked(term int) {
	if term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DError, "Can't become Follower, lower term: T%d", term)
		return
	}
	LOG(rf.me, rf.currentTerm, DLog, "%s->Follower, For T%d->T%d", rf.role, rf.currentTerm, term)

	shouldPersist := term != rf.currentTerm
	rf.role = Follower
	if term > rf.currentTerm {
		rf.votedFor = -1
	}
	rf.currentTerm = term

	// only term changed need persist
	if shouldPersist {
		rf.persistLocked()
	}
}

func (rf *Raft) becomeCandidateLocked() {
	if rf.role == Leader {
		LOG(rf.me, rf.currentTerm, DVote, "Leader can't become Candidate")
		return
	}
	LOG(rf.me, rf.currentTerm, DVote, "%s->Candidate, For T%d", rf.role, rf.currentTerm+1)
	rf.currentTerm++
	rf.role = Candidate
	rf.votedFor = rf.me
	rf.persistLocked()
	rf.resetElectionTimerLocked()
}

func (rf *Raft) becomeLeaderLocked() {
	if rf.role != Candidate {
		LOG(rf.me, rf.currentTerm, DError, "Only Candidate can become Leader")
		return
	}
	LOG(rf.me, rf.currentTerm, DLeader, "Become Leader in T%d", rf.currentTerm)
	rf.role = Leader

	// initial next/match index for this term
	for peer := 0; peer < len(rf.peers); peer++ {
		rf.nextIndex[peer] = rf.log.size()
		rf.matchIndex[peer] = 0
	}
}
