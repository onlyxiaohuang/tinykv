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
	"errors"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

// debug
const debug uint64 = 0

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

type lockedRand struct {
	mu   sync.Mutex
	rand *rand.Rand
}

var globalRand = &lockedRand{
	rand: rand.New((rand.NewSource(time.Now().UnixNano()))),
}

func (r *lockedRand) Intn(n int) int {
	r.mu.Lock()
	v := r.rand.Intn(n)
	r.mu.Unlock()
	return v
}

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
} //next待发送到该节点的下一个日志条目的索引 match已知已经同步到该节点的最高日志条目的索引

type Raft struct {
	id uint64

	Term uint64 //选举任期
	Vote uint64 //投票的id

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	// 当且只当该节点是leader才不为空,即state为leader
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	//哪些节点给自己投票了
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	//每个follower是否给了heartbeat回应，每次electionTimeout重置
	heartbeatResp map[uint64]bool
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	raftlog := newLog(c.Storage)
	hs, cs, _ := c.Storage.InitialState() //HardState and Confstate

	nodes := cs.Nodes
	if c.peers == nil {
		c.peers = nodes
	}

	//leader需要获取所有peer的节点信息，但是初始节点均为StateFollower,所以无需更改

	prs := make(map[uint64]*Progress)
	for _, pr := range c.peers {
		prs[pr] = &Progress{
			Next:  0,
			Match: 0,
		}
	}

	raft := &Raft{

		id:   c.ID,
		Term: hs.Term,
		Vote: hs.Vote,

		RaftLog: raftlog,
		State:   StateFollower, //初始都是追随者
		//		Prs:     make(map[uint64]*Progress),在becomeleader那里需要用到prs,故不能为空
		Prs:   prs,
		msgs:  make([]pb.Message, 0),
		votes: make(map[uint64]bool),
		Lead:  None,

		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		heartbeatElapsed: 0,
		electionElapsed:  0,
		leadTransferee:   0,
		PendingConfIndex: 0,

		heartbeatResp: make(map[uint64]bool),
	}

	return raft
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool { //从leader send消息到peer
	// Your Code Here (2A).
	pr := r.Prs[to]

	if debug == 1 {
		log.Printf("from %d send append to %d", r.id, to)
	}

	if pr.Next == 0 {
		return false
	}

	prevlog := pr.Next - 1 //之前log的index
	term := r.Term
	leaderid := r.id
	committedindex := r.RaftLog.committed //已经committed的index

	prevlogterm, _ := r.RaftLog.Term(prevlog)

	if debug == 1 {
		log.Printf("prevlog is %d,prevlongterm is %d", prevlog, prevlogterm)
	}

	if r.RaftLog.FirstIndex()-1 > prevlog {
		r.sendSnapshot(to)
		return false
	}
	if debug == 1 {
		log.Printf("from %d send append to %d over", r.id, to)
	}

	firstindex := r.RaftLog.FirstIndex()
	//	lastindex := r.RaftLog.LastIndex()

	var entries []*pb.Entry
	for i := pr.Next; i < r.RaftLog.LastIndex()+1; i++ {
		entries = append(entries, &r.RaftLog.entries[i-firstindex])
	}

	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    leaderid,
		Term:    term,
		LogTerm: prevlogterm,
		Index:   prevlog,
		Entries: entries,
		Commit:  committedindex,
	}

	r.msgs = append(r.msgs, msg)

	if debug == 1 {
		log.Printf("from %d send append to %d over", r.id, to)
	}

	return true
}

// 以下的三种消息均通过message发送
// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) { //by making msg
	// Your Code Here (2A).
	term := r.Term
	//pr := r.Prs[to]

	//if pr.Next == 0 {
	//	return
	//}

	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		Term:    term,
		Commit:  util.RaftInvalidIndex,
		To:      to,
		From:    r.id,
	}

	r.msgs = append(r.msgs, msg)
	return
}

// sendRequestvote sends a voting message to peer.
func (r *Raft) sendRequestVote(to uint64) {
	// Your Code Here (2A).
	_, ok := r.Prs[to]
	if !ok {
		return
	}
	term := r.Term
	lastindex := r.RaftLog.LastIndex()
	logterm, _ := r.RaftLog.Term(lastindex)
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		Term:    term,
		LogTerm: logterm,
		Index:   lastindex,
		To:      to,
		From:    r.id,
	}

	r.msgs = append(r.msgs, msg)

	return
}

// sendHeartbeat sends a Snapshot to the given peer.
func (r *Raft) sendSnapshot(to uint64) {
	// Your Code Here (2A).

	var snapshot pb.Snapshot
	var err error

	if !IsEmptySnap(r.RaftLog.pendingSnapshot) {
		snapshot = *r.RaftLog.pendingSnapshot // 挂起的还未处理的快照
	} else {
		snapshot, err = r.RaftLog.storage.Snapshot() // 生成一份快照
	}

	if err != nil {
		return
	}

	msg := pb.Message{
		MsgType:  pb.MessageType_MsgSnapshot,
		Term:     r.Term,
		Snapshot: &snapshot,
		To:       to,
		From:     r.id,
	}
	r.msgs = append(r.msgs, msg)
	r.Prs[to].Next = snapshot.Metadata.Index + 1
	return

}

func (r *Raft) sendAllRequestVote() {
	for pr := range r.Prs {
		if pr != r.id {
			r.sendRequestVote(pr)
		}
	}
}

func (r *Raft) StartElection() {

	if debug == 2 && r.id == 1 && r.Term < 10 {
		fmt.Printf("%x start elction at term %d\n", r.id, r.Term)
	}

	if _, ok := r.Prs[r.id]; !ok {
		return
	}
	if len(r.Prs) == 1 {
		r.becomeLeader()
		r.Term++
	} else {
		r.becomeCandidate()
		r.sendAllRequestVote()
	}
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).

	r.electionElapsed++
	if debug == 1 {
		log.Printf("%d's now tick is %d", r.id, r.electionElapsed)
	}
	switch r.State {
	case StateFollower:
		if r.electionElapsed >= r.electionTimeout { // 如果已经需要重新选举，那么就重新发起选举
			r.electionElapsed = 0
			err := r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
			if err != nil {
				return
			}
		}
	case StateCandidate:
		if r.electionElapsed >= r.electionTimeout { //重新选举、重置选举计时
			r.electionElapsed = 0
			err := r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
			if err != nil {
				return
			}
		}
	case StateLeader:
		r.heartbeatElapsed++ //同时还需要心跳开始计时，增加心跳的计时。
		hbrNum := len(r.heartbeatResp)
		total := len(r.Prs)

		//选举超时
		if r.electionElapsed >= r.electionTimeout {
			r.electionElapsed = 0
			r.heartbeatResp = make(map[uint64]bool)
			r.heartbeatResp[r.id] = true

			//需要查看心跳的回应数，如果小于一半，则需要重新开始选举
			if hbrNum*2 < total {
				r.StartElection()
			}

			if r.leadTransferee != None {
				r.leadTransferee = None
			}
		}

		//心跳超时
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			err := r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat})
			if err != nil {
				return
			}
		}
	}

}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.reset(term)
	r.Lead = lead

}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.reset(r.Term + 1)
	r.Vote = r.id
	r.votes[r.id] = true

}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).

	r.reset(r.Term)
	r.State = StateLeader
	r.Lead = r.id

	lastindex := r.RaftLog.LastIndex()
	for pr := range r.Prs { //此处不能让r.prs为空，需要从初始化中获取prs
		r.Prs[pr].Next = lastindex + 1
		r.Prs[pr].Match = 0
	}

	// NOTE: Leader should propose a noop entry on its term

	r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{Term: r.Term, Index: lastindex + 1})

	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
	r.Prs[r.id].Match = r.Prs[r.id].Next - 1

	for pr := range r.Prs {
		if pr != r.id {
			r.sendAppend(pr)
		}
	}

	r.updateCommitIndex()
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	//if debug == 2 && r.id == 3 {
	//	fmt.Printf("%d now start to step as a %s\n", r.id, m.MsgType)
	//}

	var err error = nil
	switch r.State {

	case StateFollower:
		err = r.FollowerStep(m)

	case StateCandidate:
		err = r.CandidateStep(m)
	case StateLeader:
		err = r.leaderStep(m)
	}
	return err
}
func (r *Raft) FollowerStep(m pb.Message) error {
	var err error = nil
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		if _, ok := r.Prs[r.id]; ok {
			r.StartElection()
		}
	case pb.MessageType_MsgBeat:
	case pb.MessageType_MsgPropose:
		err = ErrProposalDropped
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
	case pb.MessageType_MsgTransferLeader:
		if r.Lead != None {
			m.To = r.Lead
			r.msgs = append(r.msgs, m)
		}
	case pb.MessageType_MsgTimeoutNow:
		r.electionElapsed = 0
		r.StartElection()
	}
	return err
}

func (r *Raft) CandidateStep(m pb.Message) error {
	//if debug == 2 && r.id == 1 {
	//	fmt.Printf("%d now start to step as a candidate\n", r.id)
	//}

	var err error = nil
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.StartElection()
	case pb.MessageType_MsgBeat:
	case pb.MessageType_MsgPropose:
		err = ErrProposalDropped
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		total := len(r.Prs)
		agrnum := 0
		dennum := 0

		r.votes[m.From] = !m.Reject
		for _, vote := range r.votes {
			if vote {
				agrnum++
			} else {
				dennum++
			}
		}

		if 2*agrnum > total {
			r.becomeLeader()
		} else if 2*dennum >= total {
			r.becomeFollower(r.Term, None)
		}
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
	case pb.MessageType_MsgTransferLeader:
		if r.Lead != None {
			m.To = r.Lead
			r.msgs = append(r.msgs, m)
		}
	case pb.MessageType_MsgTimeoutNow:
		r.electionElapsed = 0
		r.StartElection()

	}
	return err
}

func (r *Raft) leaderStep(m pb.Message) error {
	var err error = nil
	switch m.MsgType {
	case pb.MessageType_MsgHup:
	case pb.MessageType_MsgBeat:
		for pr := range r.Prs {
			if pr != r.id {
				r.sendHeartbeat(pr)
			}
		}
	case pb.MessageType_MsgPropose:
		if r.leadTransferee == None {
			r.handlePropose(m)
		} else {
			err = ErrProposalDropped
		}
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendResponse(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHeartbeatResponse(m)
	case pb.MessageType_MsgTransferLeader:
		r.handleTransferLeader(m)
	case pb.MessageType_MsgTimeoutNow:
		r.electionElapsed = 0
		r.StartElection()
	}
	return err

}

func (r *Raft) sendAppendResponse(reject bool, to uint64, index uint64) {
	//if debug == 2 {
	//	fmt.Printf("from %d to %d, send append response\n", r.id, to)
	//}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		Term:    r.Term,
		To:      to,
		Reject:  reject,
		From:    r.id,
		Index:   index,
	}
	r.msgs = append(r.msgs, msg)
	return
}

func (r *Raft) sendHeartbeatResponse(to uint64) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		Term:    r.Term,
		To:      to,
		From:    r.id,
		Index:   r.RaftLog.committed,
	}
	r.msgs = append(r.msgs, msg)
	return
}

func (r *Raft) sendRequestVoteResponse(reject bool, to uint64) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		Term:    r.Term,
		Reject:  reject,
		To:      to,
		From:    r.id,
	}
	r.msgs = append(r.msgs, msg)
	return
}

func (r *Raft) handlePropose(m pb.Message) {
	lastindex := r.RaftLog.LastIndex()
	for i := range m.Entries {
		m.Entries[i].Term = r.Term
		m.Entries[i].Index = lastindex + 1 + uint64(i)
		r.RaftLog.entries = append(r.RaftLog.entries, *m.Entries[i])
	}
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.Prs[r.id].Match + 1

	for pr := range r.Prs {
		if pr != r.id {
			r.sendAppend(pr)
		}
	}

	if len(r.Prs) == 1 {
		r.RaftLog.commitedto(r.Prs[r.id].Match)
	}
	return
}

func (r *Raft) handleRequestVote(m pb.Message) {
	if r.Term < m.Term {
		r.Vote = None
		r.Term = m.Term
		if r.State != StateFollower {
			r.becomeFollower(r.Term, None)
		}
	}

	if m.Term < r.Term {
		r.sendRequestVoteResponse(true, m.From)
		return
	}

	if r.Vote == None || r.Vote == m.From {
		lastindex := r.RaftLog.LastIndex()
		lastterm, _ := r.RaftLog.Term(lastindex)
		if m.LogTerm > lastterm || (m.LogTerm == lastterm && m.Index >= lastindex) {
			r.sendRequestVoteResponse(false, m.From)
			r.Vote = m.From
		} else {
			r.sendRequestVoteResponse(true, m.From)
		}
	} else {
		r.sendRequestVoteResponse(true, m.From)
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	if debug == 2 {
		fmt.Printf("%d now start to step as a candidate\n", r.id)
		fmt.Printf("now term is %d, index is %d, r.entries are %d.\n", m.Term, m.Index, r.RaftLog.allEntries())
	}

	if r.Term <= m.Term {
		r.Term = m.Term
		if r.State != StateFollower {
			r.becomeFollower(r.Term, None)
		}
	}
	if r.State == StateLeader {
		return
	}

	//if m.Index > uint64(len(m.Entries)) {
	//	r.sendAppendResponse(true, m.From, r.RaftLog.LastIndex())
	//}

	if m.Term < r.Term {
		r.sendAppendResponse(true, m.From, r.RaftLog.LastIndex())
		return
	}

	if m.From != r.Lead {
		r.Lead = m.From
	}

	// unmatch with existing entry
	var existingent bool = false
	for _, ent := range r.RaftLog.allEntries() {
		if ent.Index == m.Index && ent.Term == m.LogTerm {
			existingent = true
		}
	}
	if !existingent && m.Index != 0 && m.LogTerm != 0 {
		if debug == 2 {
			fmt.Printf("unmatched entries\n")
		}
		r.sendAppendResponse(true, m.From, r.RaftLog.LastIndex())
		return
	}
	prevLogIndex := m.Index
	prevLogTerm := m.LogTerm

	if prevLogIndex > r.RaftLog.LastIndex() {
		r.sendAppendResponse(true, m.From, r.RaftLog.LastIndex())
		return
	}

	if tmpTerm, _ := r.RaftLog.Term(prevLogIndex); tmpTerm != prevLogTerm {
		r.sendAppendResponse(true, m.From, r.RaftLog.LastIndex())
	}

	for _, en := range m.Entries {
		index := en.Index
		oldterm, err := r.RaftLog.Term(index)
		if debug == 3 {
			fmt.Printf("entries en.term == %d, en.index == %d\n", en.Term, en.Index)
		}

		if index-r.RaftLog.FirstIndex() > uint64(len(r.RaftLog.entries)) || index > r.RaftLog.LastIndex() {

			r.RaftLog.entries = append(r.RaftLog.entries, *en)
			if debug == 3 {
				fmt.Printf("Now append entries en.term == %d, en.index == %d\n", en.Term, en.Index)
				for _, ents := range r.RaftLog.entries {
					fmt.Printf("after append, entries term: %d,index: %d\n", ents.Term, ents.Index)
				}
				fmt.Printf("Now r's allentries are %d, r's lastindex is %d, r entries' len is %d\n", r.RaftLog.allEntries(), r.RaftLog.LastIndex(), len(r.RaftLog.entries))
			}

		} else if oldterm != en.Term || err != nil {
			if index < r.RaftLog.FirstIndex() {
				r.RaftLog.entries = make([]pb.Entry, 0)
			} else {
				r.RaftLog.entries = r.RaftLog.entries[0 : index-r.RaftLog.FirstIndex()]
			}

			r.RaftLog.stabled = min(r.RaftLog.stabled, index-1)

			if debug == 3 {
				fmt.Printf("Now append entries en.term == %d, en.index == %d\n", en.Term, en.Index)
			}
			r.RaftLog.entries = append(r.RaftLog.entries, *en)

		}
	}

	r.RaftLog.lastAppend = m.Index + uint64(len(m.Entries))

	r.sendAppendResponse(false, m.From, r.RaftLog.LastIndex())

	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(m.Commit, r.RaftLog.lastAppend)
	}
	return

}

func (r *Raft) handleAppendResponse(m pb.Message) {
	if m.Reject {
		r.Prs[m.From].Next = min(m.Index+1, r.Prs[m.From].Next-1)
		r.sendAppend(m.From)
		return
	}

	r.Prs[m.From].Match = m.Index
	r.Prs[m.From].Next = m.Index + 1

	oldcom := r.RaftLog.committed
	r.updateCommitIndex()

	if r.RaftLog.committed != oldcom {
		for pr := range r.Prs {
			if pr != r.id {
				r.sendAppend(pr)
			}
		}
	}

	if m.From == r.leadTransferee {
		r.Step(pb.Message{MsgType: pb.MessageType_MsgTransferLeader, From: m.From})
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	if r.Term <= m.Term {
		r.Term = m.Term
		if r.State != StateFollower {
			r.becomeFollower(r.Term, None)
		}
	}

	if m.From != r.Lead {
		r.Lead = m.From
	}
	r.electionElapsed = 0
	r.sendHeartbeatResponse(m.From)
	return
}

func (r *Raft) handleHeartbeatResponse(m pb.Message) {

	if r.Term < m.Term {
		r.Term = m.Term
		if r.State != StateFollower {
			r.becomeFollower(r.Term, None)
		}
	}
	r.heartbeatResp[m.From] = true

	if m.Commit < r.RaftLog.committed {
		r.sendAppend(m.From)
	}
}

func (r *Raft) handleTransferLeader(m pb.Message) {
	if r.State != StateLeader {
		return
	}

	if _, ok := r.Prs[m.From]; !ok {
		return
	}
	r.leadTransferee = m.From

	if r.Prs[m.From].Match == r.RaftLog.LastIndex() {
		msg := pb.Message{
			MsgType: pb.MessageType_MsgTimeoutNow,
			From:    r.id,
			To:      m.From,
		}
		r.msgs = append(r.msgs, msg)
	} else {
		r.sendAppend(m.From)
	}

}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

/*
 */
func (r *Raft) reset(term uint64) {
	if r.Term != term {
		r.Term = term
		r.Vote = None
	}

	r.Lead = None
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.resetRandomizedElectionTimeout()
	r.leadTransferee = None
	r.Vote = None
	r.votes = make(map[uint64]bool)
	r.heartbeatResp = make(map[uint64]bool)
	r.heartbeatResp[r.id] = true
}

func (r *Raft) resetRandomizedElectionTimeout() {
	rand := globalRand.Intn(r.electionTimeout) //可以直接当前秒数mod20 + 10
	r.electionTimeout = rand%20 + 10
}

func (r *Raft) updateCommitIndex() uint64 {
	match := make(uint64Slice, len(r.Prs))
	i := 0
	for _, prs := range r.Prs {
		match[i] = prs.Match
		i++
	}
	sort.Sort(match)

	//回到与r相符的任期
	maxN := match[(len(r.Prs)-1)/2]
	N := maxN
	for ; N > r.RaftLog.committed; N-- {
		if term, _ := r.RaftLog.Term(N); term == r.Term {
			break
		}
	}
	r.RaftLog.committed = N
	return N

}

func (r *Raft) softState() *SoftState { return &SoftState{Lead: r.Lead, RaftState: r.State} }

func (r *Raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}
