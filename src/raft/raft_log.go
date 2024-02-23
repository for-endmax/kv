package raft

import (
	"course/labgob"
	"fmt"
)

type RaftLog struct {
	//	contains [1,snapLastIdx]
	snapshot []byte
	//	contains (snapLastIdx,snapLastIdx+len(tailLog)-1]
	// taliLog[0] is a dummy entry
	tailLog []LogEntry

	snapLastIdx  int
	snapLastTerm int
}

func NewLog(snapLastIndex int, snapLastTerm int, snapshot []byte, entries []LogEntry) *RaftLog {
	rl := &RaftLog{
		snapshot:     snapshot,
		snapLastIdx:  snapLastIndex,
		snapLastTerm: snapLastTerm,
	}
	rl.tailLog = append(rl.tailLog, LogEntry{
		Term: snapLastTerm,
	})
	rl.tailLog = append(rl.tailLog, entries...)
	return rl
}

//	Locked

// return detailed error for the caller to log
func (rl *RaftLog) readPersist(d *labgob.LabDecoder) error {
	var lastIdx int
	if err := d.Decode(&lastIdx); err != nil {
		return fmt.Errorf("decode last include index failed")
	}
	rl.snapLastIdx = lastIdx

	var lastTerm int
	if err := d.Decode(&lastTerm); err != nil {
		return fmt.Errorf("decode last include term failed")
	}
	rl.snapLastTerm = lastTerm

	var log []LogEntry
	if err := d.Decode(&log); err != nil {
		return fmt.Errorf("decode tail log failed")
	}
	rl.tailLog = log

	return nil
}

func (rl *RaftLog) persist(e *labgob.LabEncoder) {
	e.Encode(rl.snapLastIdx)
	e.Encode(rl.snapLastTerm)
	e.Encode(rl.tailLog)
}

// the dummy log is counted
func (rl *RaftLog) size() int {
	return rl.snapLastIdx + len(rl.tailLog)
}

// access the index `rl.snapLastIdx` is allowed, although it's not exist, actually.
func (rl *RaftLog) idx(logicIdx int) int {
	if logicIdx < rl.snapLastIdx || logicIdx >= rl.size() {
		panic(fmt.Sprintf("%d is out of [%d, %d]", logicIdx, rl.snapLastIdx+1, rl.size()-1))
	}
	return logicIdx - rl.snapLastIdx
}

func (rl *RaftLog) at(logicIdx int) LogEntry {
	return rl.tailLog[rl.idx(logicIdx)]
}

func (rl *RaftLog) last() (index int, term int) {
	return rl.size() - 1, rl.at(rl.size() - 1).Term
}

func (rl *RaftLog) firstFor(term int) int {
	for idx, entry := range rl.tailLog {
		if entry.Term == term {
			return idx + rl.snapLastIdx
		} else if entry.Term > term {
			break
		}
	}
	return InvalidIndex
}

func (rl *RaftLog) lastFor(term int) int {
	lastIndex := InvalidIndex
	for idx, entry := range rl.tailLog {
		if entry.Term == term {
			lastIndex = idx + rl.snapLastIdx
		} else if entry.Term > term {
			break
		}
	}
	return lastIndex
}

func (rl *RaftLog) String() string {
	var terms string
	prevTerm := rl.snapLastTerm
	prevStart := 0
	for i := 0; i < len(rl.tailLog); i++ {
		if rl.tailLog[i].Term != prevStart {
			terms += fmt.Sprintf(" [%d,%d]T%d", prevStart+rl.snapLastIdx, rl.snapLastIdx+i-1, prevTerm)
			prevStart = i
			prevTerm = rl.tailLog[i].Term
		}
	}
	terms += fmt.Sprintf("[%d,%d]T%d", prevStart+rl.snapLastIdx, rl.snapLastIdx+len(rl.tailLog)-1, prevTerm)
	return terms
}

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

func (rl *RaftLog) append(e LogEntry) {
	rl.tailLog = append(rl.tailLog, e)
}

func (rl *RaftLog) appendFrom(logicPrevIndex int, entries []LogEntry) {
	rl.tailLog = append(rl.tailLog[:rl.idx(logicPrevIndex)+1], entries...)
}

func (rl *RaftLog) tail(prevIdx int) []LogEntry {
	if prevIdx+1 >= rl.size() {
		return nil
	}
	return rl.tailLog[rl.idx(prevIdx)+1:]
}
