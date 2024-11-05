// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"fmt"
	"log"
	"math"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	lastAppend uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	if storage == nil {
		log.Panic("storage cannot be nil")
	}

	raft := new(RaftLog)
	raft.storage = storage
	firstindex, _ := storage.FirstIndex()
	lastindex, _ := storage.LastIndex()

	hs, _, _ := storage.InitialState()

	raft.committed = hs.Commit
	raft.applied = firstindex - 1
	raft.stabled = lastindex

	raft.entries, _ = storage.Entries(firstindex, lastindex+1)

	//(2C) initialize pendingSnapshot
	raft.pendingSnapshot = nil

	raft.lastAppend = math.MaxInt64
	return raft
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	entries := make([]pb.Entry, 0)
	if len(l.entries) > 0 {
		firstindex := l.FirstIndex()
		lastindex := l.LastIndex()
		if l.stabled < firstindex { //storage全是不稳定的
			entries = l.entries
		}
		if l.stabled < lastindex && l.stabled >= firstindex { // firstindex <= stabled < lastindex
			entries = l.entries[l.stabled-firstindex+1:]
		}
	}

	return entries
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	firstIndex := l.FirstIndex()
	appliedIndex := l.applied
	commitedIndex := l.committed
	if len(l.entries) > 0 {
		if appliedIndex >= firstIndex-1 && commitedIndex >= firstIndex-1 && appliedIndex < commitedIndex && commitedIndex <= l.LastIndex() {
			return l.entries[appliedIndex-firstIndex+1 : commitedIndex-firstIndex+1]
		}
	}
	return make([]pb.Entry, 0)
}

func (l *RaftLog) FirstIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) == 0 { //initialize
		index, _ := l.storage.FirstIndex()
		return index
	}
	return l.entries[0].Index
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) == 0 { // initialize
		index, _ := l.storage.LastIndex()
		return index
	}
	return l.entries[len(l.entries)-1].Index
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		term, err := l.storage.Term(i)
		//if debug == 1 {
		//	log.Printf("term is %d", term)
		//}

		if err != nil {
			return 0, err
		}
		return term, nil
	}
	firstindex := l.FirstIndex()
	lastindex := l.LastIndex()
	if i >= firstindex && i <= lastindex {
		return l.entries[i-firstindex].Term, nil
	}

	return 0, nil
}

func (l *RaftLog) appliedto(to uint64) {
	l.applied = to
}
func (l *RaftLog) commitedto(to uint64) {
	if l.committed < to {
		l.committed = to
	}
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	// Your Code Here (2A).

	firstindex := l.FirstIndex()
	lastindex := l.LastIndex()

	if debug == 3 {
		fmt.Printf("first index is %d,last index is %d\n", firstindex, lastindex)
	}

	return l.entries
}
