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
	"bytes"
	"encoding/gob"
	"labrpc"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

// machine role
type Role int

const (
	LEADER = Role(iota)
	FOLLOWER
	CANDIDATE
)

func (role Role) String() string {
	switch role {
	case LEADER:
		return "Leader"
	case FOLLOWER:
		return "Follower"
	case CANDIDATE:
		return "Candidate"
	default:
		return "Unknown"
	}
}

const (
	MIN_TIMEOUT        = 150
	MAX_TIMEOUT        = 300
	HEARTBEAT_INTERVAL = 50
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistant state
	currentTerm int
	votedFor    int
	log         []LogEntry

	// volatile state on all servers
	commitIndex int
	lastApplied int

	// volatile state on leaders
	nextIndex  []int
	matchIndex []int

	role Role

	timer *time.Timer

	chanApply chan bool
	applyCh   chan ApplyMsg
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term      int
	Success   bool
	NextIndex int
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term > rf.currentTerm {
		rf.convert2Follower(args.Term)
	}
	if args.Term == rf.currentTerm && rf.role == CANDIDATE {
		rf.convert2Follower(args.Term)
	}
	if args.Term < rf.currentTerm {
		return
	}
	rf.resetST()

	if args.PrevLogIndex > rf.getLastLogIndex() {
		reply.NextIndex = rf.getLastLogIndex() + 1
		return
	}

	// for optimize
	firstIndex := rf.log[0].Index
	if args.PrevLogIndex >= firstIndex && len(rf.log) > 0 {
		tempTerm := rf.log[args.PrevLogIndex-firstIndex].Term
		if args.PrevLogTerm != tempTerm {
			for i := args.PrevLogIndex - 1; i >= firstIndex; i-- {
				if rf.log[i-firstIndex].Term != tempTerm {
					reply.NextIndex = i + 1
					break
				}
			}
			return
		}

		rf.log = rf.log[:args.PrevLogIndex+1-firstIndex]
		rf.log = append(rf.log, args.Entries...)
		if len(args.Entries) != 0 {
			// DPrintf("Server [%v] LogEntries: [%v]", rf.me, rf.log)
		}

		reply.Success = true
		reply.NextIndex = rf.getLastLogIndex() + 1
	}

	if args.LeaderCommit > rf.commitIndex {
		if lastIndex := rf.getLastLogIndex(); lastIndex > args.LeaderCommit {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastIndex
		}
		rf.chanApply <- true
	}
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isleader bool

	term = rf.currentTerm
	isleader = rf.role == LEADER

	return term, isleader
}

func (rf *Raft) GetPersistSize() int {
	return rf.persister.RaftStateSize()
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) sendInstallSnapshot(server int, args InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) InstallSnapshot(args InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	rf.persister.SaveSnapshot(args.Data)
	rf.log = rf.truncateLog(args.LastIncludedIndex, args.LastIncludedTerm)
	rf.lastApplied = args.LastIncludedIndex
	rf.commitIndex = args.LastIncludedIndex
	rf.persist()

	msg := ApplyMsg{UseSnapshot: true, Snapshot: args.Data}
	rf.applyCh <- msg
}

func (rf *Raft) readSnapshot(data []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if len(data) == 0 {
		return
	}

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)

	var LastIncludedIndex int
	var LastIncludedTerm int

	d.Decode(&LastIncludedIndex)
	d.Decode(&LastIncludedTerm)

	rf.commitIndex = LastIncludedIndex
	rf.lastApplied = LastIncludedIndex
	rf.log = rf.truncateLog(LastIncludedIndex, LastIncludedTerm)
	rf.persist()

	msg := ApplyMsg{UseSnapshot: true, Snapshot: data}
	go func() {
		rf.applyCh <- msg
	}()
}

func (rf *Raft) truncateLog(lastIncludedIndex, lastIncludedTerm int) []LogEntry {
	var newLogs []LogEntry
	newLogs = append(newLogs, LogEntry{Index: lastIncludedIndex, Term: lastIncludedTerm})

	for i := len(rf.log) - 1; i >= 0; i-- {
		if rf.log[i].Index == lastIncludedIndex && rf.log[i].Term == lastIncludedTerm {
			newLogs = append(newLogs, rf.log[i+1:]...)
			break
		}
	}

	return newLogs
}

func (rf *Raft) SaveSnapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	firstIndex := rf.log[0].Index
	lastIndex := rf.getLastLogIndex()

	if index <= firstIndex || index > lastIndex {
		return
	}

	rf.log[index-firstIndex] = LogEntry{Index: index, Term: rf.log[index-firstIndex].Term}
	rf.log = rf.log[index-firstIndex:]
	rf.persist()

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.log[0].Index)
	e.Encode(rf.log[0].Term)
	data := w.Bytes()
	data = append(data, snapshot...)
	rf.persister.SaveSnapshot(data)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if args.Term > rf.currentTerm {
		rf.convert2Follower(args.Term)
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		return
	}

	lastLogTerm := rf.getLastLogTerm()
	lastLogIndex := rf.getLastLogIndex()
	// voted-none && least-up-to-date

	up2Date := false
	if lastLogTerm < args.LastLogTerm {
		up2Date = true
	}
	if lastLogTerm == args.LastLogTerm && lastLogIndex <= args.LastLogIndex {
		up2Date = true
	}

	if up2Date && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		// DPrintf("Server [%v] vote [%v] for Term [%v]", rf.me, args.CandidateId, rf.currentTerm)
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	rf.mu.Lock()
	defer rf.mu.Unlock()

	isLeader = rf.role == LEADER
	term = rf.currentTerm

	if isLeader {
		index = rf.getLastLogIndex() + 1
		log := LogEntry{
			Index:   index,
			Term:    rf.currentTerm,
			Command: command,
		}
		rf.log = append(rf.log, log)

		rf.persist()
	}

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) resetST() {
	rf.stopST()
	rf.timer.Reset(rf.getRandomST() * time.Millisecond)
}

func (rf *Raft) stopST() {
	if !rf.timer.Stop() {
		select {
		case <-rf.timer.C:
		default:
		}
	}
}

func (rf *Raft) getRandomST() time.Duration {
	return time.Duration(rand.Intn(MAX_TIMEOUT-MIN_TIMEOUT) + MIN_TIMEOUT)
}

func (rf *Raft) getLastLogIndex() int {
	return rf.log[len(rf.log)-1].Index
}

func (rf *Raft) getLastLogTerm() int {
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) getPrevLogIndex(server int) int {
	return rf.nextIndex[server] - 1
}

func (rf *Raft) getPrevLogTerm(server int) int {
	prevLogIndex := rf.getPrevLogIndex(server)
	return rf.log[prevLogIndex-rf.log[0].Index].Term
}

func (rf *Raft) election() {
	for {
		<-rf.timer.C
		// DPrintf("Server [%v] expire, currentTerm [%v]", rf.me, rf.currentTerm)

		rf.mu.Lock()
		rf.role = CANDIDATE
		rf.currentTerm++
		rf.votedFor = rf.me
		rf.persist()
		rf.resetST()

		args := RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: rf.getLastLogIndex(),
			LastLogTerm:  rf.getLastLogTerm(),
		}
		rf.mu.Unlock()

		var numVotedForMe int32 = 1
		for i, _ := range rf.peers {
			if i == rf.me {
				continue
			}

			go func(rf *Raft, server int) {
				reply := &RequestVoteReply{}
				ret := rf.sendRequestVote(server, args, reply)
				if ret == true {
					rf.mu.Lock()
					if rf.role != CANDIDATE {
						rf.mu.Unlock()
						return
					}

					if reply.VoteGranted == true {
						atomic.AddInt32(&numVotedForMe, 1)
					}

					if reply.Term > rf.currentTerm {
						rf.convert2Follower(reply.Term)
						rf.mu.Unlock()
						return
					}
					rf.mu.Unlock()

					if atomic.LoadInt32(&numVotedForMe) > int32(len(rf.peers)/2) {
						rf.convert2Leader()
					}
				}
			}(rf, i)
		}
	}
}

func (rf *Raft) convert2Leader() {
	// DPrintf("Convert server [%v] role [%v] -> [Leader] for Term [%v]", rf.me, rf.role.String(), rf.currentTerm)

	rf.mu.Lock()
	rf.role = LEADER
	rf.stopST()

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i, _ := range rf.peers {
		rf.nextIndex[i] = rf.getLastLogIndex() + 1
		rf.matchIndex[i] = 0
	}
	rf.mu.Unlock()

	// send heartbeat
	timer := time.NewTicker(HEARTBEAT_INTERVAL * time.Millisecond)
	for range timer.C {
		rf.mu.Lock()
		changeFlag := false
		firstIndex := rf.log[0].Index
		for i := rf.commitIndex + 1; i <= rf.getLastLogIndex(); i++ {
			count := 1
			for j, _ := range rf.peers {
				if j == rf.me {
					continue
				}
				if rf.log[i-firstIndex].Term == rf.currentTerm && rf.matchIndex[j] >= i {
					count++
				}
			}
			if count > len(rf.peers)/2 {
				changeFlag = true
				rf.commitIndex = i
			}
		}
		if changeFlag {
			rf.chanApply <- true
		}
		rf.mu.Unlock()

		for i, _ := range rf.peers {
			if i == rf.me {
				continue
			}
			rf.mu.Lock()
			if rf.role != LEADER {
				rf.mu.Unlock()
				return
			}
			firstIndex = rf.log[0].Index
			if rf.nextIndex[i] > firstIndex {
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: rf.getPrevLogIndex(i),
					PrevLogTerm:  rf.getPrevLogTerm(i),
					LeaderCommit: rf.commitIndex,
				}

				if len(rf.log) > 0 {
					args.Entries = make([]LogEntry, len(rf.log[rf.nextIndex[i]-firstIndex:]))
					copy(args.Entries, rf.log[rf.nextIndex[i]-firstIndex:])
				}
				rf.mu.Unlock()

				reply := &AppendEntriesReply{}
				go func(rf *Raft, server int) {
					if ret := rf.sendAppendEntries(server, args, reply); ret {
						rf.handleAppendEntries(server, args, *reply)
					}
				}(rf, i)
			} else if firstIndex > 0 {
				args := InstallSnapshotArgs{
					Term:              rf.currentTerm,
					LeaderId:          rf.me,
					LastIncludedIndex: rf.log[0].Index,
					LastIncludedTerm:  rf.log[0].Term,
					Data:              rf.persister.ReadSnapshot(),
				}
				rf.mu.Unlock()

				reply := &InstallSnapshotReply{}
				go func(rf *Raft, server int) {
					if ret := rf.sendInstallSnapshot(server, args, reply); ret {
						rf.handleInstallSnapshot(server, args, *reply)
					}
				}(rf, i)
			} else {
				rf.mu.Unlock()
			}
		}
	}
}

func (rf *Raft) handleInstallSnapshot(server int, args InstallSnapshotArgs, reply InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.convert2Follower(reply.Term)
		return
	}

	rf.nextIndex[server] = args.LastIncludedIndex + 1
	rf.matchIndex[server] = args.LastIncludedIndex
}

func (rf *Raft) handleAppendEntries(server int, args AppendEntriesArgs, reply AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.convert2Follower(reply.Term)
		return
	}
	if reply.Success == true {
		if len(args.Entries) > 0 {
			rf.nextIndex[server] = args.Entries[len(args.Entries)-1].Index + 1
			rf.matchIndex[server] = rf.nextIndex[server] - 1
		}
	} else if reply.NextIndex > 0 {
		rf.nextIndex[server] = reply.NextIndex
	}
}

func (rf *Raft) convert2Follower(term int) {
	// DPrintf("Convert server [%v] role [%v] -> [Follower] for Term [%v] -> [%v]", rf.me, rf.role.String(), rf.currentTerm, term)

	rf.role = FOLLOWER
	rf.currentTerm = term
	rf.votedFor = -1
	rf.persist()
	rf.resetST()
}

func (rf *Raft) apply(applyCh chan ApplyMsg) {
	for {
		select {
		case <-rf.chanApply:
			rf.mu.Lock()
			firstIndex := rf.log[0].Index
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				msg := ApplyMsg{
					Index:   i,
					Command: rf.log[i-firstIndex].Command,
				}
				applyCh <- msg
				rf.lastApplied = i
			}
			rf.mu.Unlock()
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = append(rf.log, LogEntry{Term: 0, Index: 0})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.role = FOLLOWER
	rf.timer = time.NewTimer(rf.getRandomST() * time.Millisecond)

	rf.chanApply = make(chan bool)
	rf.applyCh = applyCh

	rf.readSnapshot(persister.ReadSnapshot())

	go rf.election()
	go rf.apply(applyCh)

	return rf
}
