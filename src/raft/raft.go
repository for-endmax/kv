package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"sync"
	"sync/atomic"
	"time"

	//	"course/labgob"
	"course/labrpc"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (PartA, PartB, PartC).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role        Role
	currentTerm int
	votedFor    int // -1 means vote for none

	log        *RaftLog
	nextIndex  []int
	matchIndex []int

	// fields for applying loop
	applyCh     chan ApplyMsg
	applyCond   *sync.Cond
	snapPending bool
	commitIndex int
	lastApplied int

	electionStart   time.Time
	electionTimeout time.Duration // random
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (PartA).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.role == Leader
	rf.mu.Unlock()
	return term, isleader
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (PartD).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// check index valid
	if index > rf.commitIndex {
		LOG(rf.me, rf.currentTerm, DSnap, "Couldn't snapshot before CommitIdx: %d>%d", index, rf.commitIndex)
		return
	}
	if index <= rf.log.snapLastIdx {
		LOG(rf.me, rf.currentTerm, DSnap, "Already snapshot in %d<=%d", index, rf.log.snapLastIdx)
		return
	}
	rf.log.doSnapshot(index, snapshot)

	rf.persistLocked()
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != Leader {
		return 0, 0, false
	}
	rf.log.append(LogEntry{
		CommandValid: true,
		Command:      command,
		Term:         rf.currentTerm,
	})
	rf.persistLocked()
	LOG(rf.me, rf.currentTerm, DLeader, "Leader accept log [%d]T%d", rf.log.size()-1, rf.currentTerm)

	return rf.log.size() - 1, rf.currentTerm, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (PartA, PartB, PartC).
	rf.role = Follower
	rf.currentTerm = 1
	rf.votedFor = -1

	rf.log = NewLog(InvalidIndex, InvalidTerm, nil, nil)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	//initialize the fields used for apply
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.snapPending = false

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.electionTicker()

	// start ticker goroutine to start application
	go rf.applicationTicker()

	return rf
}

func (rf *Raft) contextLostLocked(role Role, term int) bool {
	return !(rf.currentTerm == term && rf.role == role)
}
