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
	"labgob"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// When ApplySnapshot is true, the state should be reset to SnapshotState
	ApplySnapshot bool
	SnapshotState []byte
}

type Log struct {
	Command interface{}
	Term    int
}

// enum type for the status of raft instance
type status int

const (
	leader status = iota
	candidate
	follower
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	applyCh   chan ApplyMsg

	// utils
	currentStatus   status
	wakeApplyCh     chan struct{}
	updateTimeoutCh chan struct{}
	leaderChangeCh  chan struct{}
	// persistent
	currentTerm int
	log         []Log
	votedFor    int
	// volatile
	commitIndex int
	lastApplied int
	// volatile on leader
	nextIndex  []int
	matchIndex []int
	// snapshot
	haveSnapshot  bool
	snapshotState []byte
	snapshotIndex int
	snapshotTerm  int

	killed bool
}

const appendEntriesInterval time.Duration = time.Millisecond * 100
const timeOut int = 1000

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.currentStatus == leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.haveSnapshot)

	rf.persister.SaveRaftState(w.Bytes())
}

func (rf *Raft) persistSnapshot() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.haveSnapshot)
	ws := new(bytes.Buffer)
	e = labgob.NewEncoder(ws)
	e.Encode(rf.snapshotState)
	e.Encode(rf.snapshotIndex)
	e.Encode(rf.snapshotTerm)

	rf.persister.SaveStateAndSnapshot(w.Bytes(), ws.Bytes())
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []Log
	var haveSnapshot bool
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&haveSnapshot) != nil {
		panic("Decode Failed!")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.haveSnapshot = haveSnapshot
	}
}

func (rf *Raft) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var snapshotState []byte
	var snapshotIndex int
	var snapshotTerm int
	if d.Decode(&snapshotState) != nil ||
		d.Decode(&snapshotIndex) != nil ||
		d.Decode(&snapshotTerm) != nil {
		panic("Decode Failed!")
	}
	rf.snapshotState = snapshotState
	rf.snapshotIndex = snapshotIndex
	rf.snapshotTerm = snapshotTerm
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term     int
	Success  bool
	FailTerm int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.updateTimeout()

	if rf.currentTerm < args.Term {
		rf.turnFollower(args.Term)
	}

	if rf.currentStatus != follower {
		assert(rf.currentTerm == args.Term)
		assert(rf.currentStatus == candidate)
		rf.turnFollower(args.Term)
	}

	// bit of hack. If prevLogIndex < snapshotIndex, make it look like prevLogIndex = snapshotIndex
	if args.PrevLogIndex < rf.snapshotIndex {
		if args.PrevLogIndex + len(args.Entries) <= rf.snapshotIndex {
			reply.Term = rf.currentTerm
			reply.Success = true
			return
		}
		args.Entries = args.Entries[rf.snapshotIndex-args.PrevLogIndex:]
		args.PrevLogIndex = rf.snapshotIndex
	}

	if len(rf.log)+rf.snapshotIndex <= args.PrevLogIndex || rf.log[args.PrevLogIndex-rf.snapshotIndex].Term != args.PrevLogTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		if len(rf.log)+rf.snapshotIndex-1 <= args.PrevLogIndex {
			reply.FailTerm = rf.log[len(rf.log)-1].Term
		} else {
			reply.FailTerm = rf.log[args.PrevLogIndex-rf.snapshotIndex].Term
		}
		return
	}

	var match int
	// delete conflicting entries...
	for match = 0; match < len(rf.log)+rf.snapshotIndex-args.PrevLogIndex-1 && match < len(args.Entries); match++ {
		if rf.log[args.PrevLogIndex-rf.snapshotIndex+match+1].Term != args.Entries[match].Term {
			rf.log = rf.log[:args.PrevLogIndex-rf.snapshotIndex+match+1]
			break
		}
	}
	// append entries... might have matched all
	if match < len(args.Entries) {
		rf.log = append(rf.log, args.Entries[match:]...)
	}

	// update commit
	if args.LeaderCommit > rf.commitIndex {
		lastIndex := len(rf.log) + rf.snapshotIndex - 1
		rf.commitIndex = min(lastIndex, args.LeaderCommit)
		rf.tryWakeApply()
	}
	reply.Term = rf.currentTerm
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if rf.currentTerm < args.Term {
		rf.turnFollower(args.Term)
	}

	lastLogIndex := len(rf.log) + rf.snapshotIndex - 1
	lastLogTerm := rf.log[len(rf.log)-1].Term
	// Candidate at least as up-to-date
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(lastLogTerm < args.LastLogTerm ||
			(lastLogTerm == args.LastLogTerm && lastLogIndex <= args.LastLogIndex)) {
		rf.updateTimeout()
		rf.votedFor = args.CandidateId
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
	} else {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}
}

type InstallSnapshotArgs struct {
	Term          int
	State         []byte
	SnapshotIndex int
	SnapshotTerm  int
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	DPrintf("Server %d is installling snapshot...", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persistSnapshot()

	assert(args != nil)
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}

	prevSnapshotIndex := rf.snapshotIndex

	if args.SnapshotIndex > rf.snapshotIndex {
		rf.haveSnapshot = true
		rf.snapshotState = args.State
		rf.snapshotIndex = args.SnapshotIndex
		rf.snapshotTerm = args.SnapshotTerm
	} else {
		return
	}

	if args.SnapshotIndex < len(rf.log)+prevSnapshotIndex-1 && rf.log[args.SnapshotIndex-prevSnapshotIndex].Term == args.SnapshotTerm {
		newLog := make([]Log, 1)
		rf.log = append(newLog, rf.log[args.SnapshotIndex+1-prevSnapshotIndex:]...)
		return
	}

	rf.log = make([]Log, 1)
	rf.log[0].Term = rf.snapshotTerm

	rf.applySnapshot()
	rf.updateTimeout()
}

func (rf *Raft) SaveSnapshot(state []byte, index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persistSnapshot()
	DPrintf("Server %d is saving snapshot", rf.me)

	prevSnapshotIndex := rf.snapshotIndex
	rf.haveSnapshot = true
	rf.snapshotState = state
	rf.snapshotIndex = index
	rf.snapshotTerm = rf.log[index-prevSnapshotIndex].Term
	newLog := make([]Log, 1)
	newLog[0].Term = rf.snapshotTerm
	rf.log = append(newLog, rf.log[index-prevSnapshotIndex+1:]...)

}

func (rf *Raft) applySnapshot() {
	rf.applyCh <- ApplyMsg{
		ApplySnapshot: true,
		SnapshotState: rf.snapshotState,
	}
	rf.lastApplied = rf.snapshotIndex
	if rf.commitIndex < rf.lastApplied {
		rf.commitIndex = rf.lastApplied
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
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var isLeader bool
	term := rf.currentTerm
	index := len(rf.log) + rf.snapshotIndex
	if rf.currentStatus == leader {
		isLeader = true
		rf.log = append(rf.log, Log{command, rf.currentTerm})
		rf.persist()
		// append immdiately
		rf.appendEntries()
	} else {
		isLeader = false
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
	rf.mu.Lock()
	rf.killed = true
	rf.mu.Unlock()
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
	rf.applyCh = applyCh
	rf.me = me
	// state
	rf.currentTerm = 1
	rf.currentStatus = follower
	rf.log = make([]Log, 1)
	rf.commitIndex = 0
	rf.lastApplied = 0
	// utils
	rf.wakeApplyCh = make(chan struct{}, 1)
	rf.updateTimeoutCh = make(chan struct{}, 1)
	rf.leaderChangeCh = make(chan struct{}, 1)
	// snapshot
	rf.haveSnapshot = false
	rf.snapshotIndex = 0
	rf.snapshotTerm = 0

	rf.killed = false

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	if rf.haveSnapshot {
		rf.readSnapshot(persister.ReadSnapshot())
		rf.applySnapshot()
	}
	rf.run()
	return rf
}

func (rf *Raft) run() {
	// Start long-running goroutines
	go rf.sendHeartBeatsThread()
	go rf.tryApplyThread()
	go rf.detectTimeOutThread()
}

// Started as separate goroutine at start. Periodically check if one's leader and send
// AppendEntries RPCs if is.
func (rf *Raft) sendHeartBeatsThread() {
	for {
		if rf.killed {
			break
		}
		time.Sleep(appendEntriesInterval)
		rf.mu.Lock()
		if rf.currentStatus == leader {
			rf.appendEntries()
		}
		rf.mu.Unlock()
	}
}

// Send AppendEntries RPC to all peers. Called by leader. Should be called when holding lock.
func (rf *Raft) appendEntries() {
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			rf.appendEntriesTo(i)
		}
	}
}

func (rf *Raft) appendEntriesTo(server int) {
	thisTerm := rf.currentTerm
	// make a local copy of everything that's going to be needed for the goroutine
	// will raise race conditions if not done
	if rf.nextIndex[server] <= rf.snapshotIndex {
		return
	}
	prevLogIndex := rf.nextIndex[server] - 1
	prevLogTerm := rf.log[prevLogIndex-rf.snapshotIndex].Term
	entriesLen := len(rf.log) + rf.snapshotIndex - rf.nextIndex[server]
	entries := make([]Log, entriesLen)
	copy(entries, rf.log[rf.nextIndex[server]-rf.snapshotIndex:])
	thisNextIndex := rf.nextIndex[server]
	thisMatchIndex := rf.matchIndex[server]
	commitIndex := rf.commitIndex
	assert(prevLogIndex < len(rf.log)+rf.snapshotIndex)
	assert(prevLogIndex >= 0)

	args := AppendEntriesArgs{
		Term:         thisTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: commitIndex,
	}

	// spawn a separate goroutine for handling RPC with each peer
	go func() {
		var reply AppendEntriesReply
		// set entries according to prevLogIndex
		ok := rf.sendAppendEntries(server, &args, &reply)
		if !ok {
			return
		}

		rf.mu.Lock()
		defer rf.mu.Unlock()
		// check if reply is out-dated
		if reply.Term > rf.currentTerm {
			rf.turnFollower(reply.Term)
			return
		}
		if rf.currentTerm != thisTerm {
			return
		}
		// handle reply
		if reply.Success {
			// try to update matchIndex and nextIndex
			rf.matchIndex[server] = max(thisNextIndex+len(entries)-1, rf.matchIndex[server])
			rf.nextIndex[server] = max(thisNextIndex+len(entries), rf.nextIndex[server])
			// try to commit new logs
			for index := rf.commitIndex + 1; index < len(rf.log)+rf.snapshotIndex; index++ {
				if rf.log[index-rf.snapshotIndex].Term == rf.currentTerm {
					matchCount := 1
					for i := 0; i < len(rf.peers); i++ {
						if i != rf.me && rf.matchIndex[i] >= index {
							matchCount++
						}
					}
					if matchCount > len(rf.peers)/2 {
						rf.commitIndex = index
					} else {
						break
					}
				}
			}
			rf.tryWakeApply()
		} else {
			if thisTerm < reply.Term {
				rf.turnFollower(reply.Term)
			} else {
				// try to decrease nextIndex to match follower
				if rf.nextIndex[server] != thisNextIndex || rf.matchIndex[server] != thisMatchIndex {
					return
				}
				if reply.FailTerm < args.PrevLogTerm {
					for rf.nextIndex[server] > rf.snapshotIndex && rf.log[rf.nextIndex[server]-rf.snapshotIndex-1].Term > reply.FailTerm {
						rf.nextIndex[server]--
					}
				} else {
					rf.nextIndex[server] = min(rf.nextIndex[server], prevLogIndex)
				}

				// assert(rf.nextIndex[server] >= rf.snapshotIndex)
				if rf.nextIndex[server]-1 <= rf.snapshotIndex && rf.haveSnapshot {
					go rf.sendInstallSnapshotTo(server)
				} else{
					rf.appendEntriesTo(server)
				}
			}
		}
	}()
}

func (rf *Raft) sendInstallSnapshotTo(server int) {
	var args InstallSnapshotArgs
	var reply InstallSnapshotReply
	rf.mu.Lock()
	if rf.currentStatus != leader {
		rf.mu.Unlock()
		return
	}
	args.Term = rf.currentTerm
	args.State = rf.snapshotState
	args.SnapshotIndex = rf.snapshotIndex
	args.SnapshotTerm = rf.snapshotTerm
	thisSnapshotIndex := rf.snapshotIndex
	rf.mu.Unlock()

	ok := rf.sendInstallSnapshot(server, &args, &reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !ok {
		return
	}
	if rf.currentStatus != leader {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.turnFollower(reply.Term)
		return
	}
	rf.nextIndex[server] = max(rf.nextIndex[server], thisSnapshotIndex+1)
	rf.matchIndex[server] = max(rf.matchIndex[server], thisSnapshotIndex)
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	DPrintf("Server %d call %d installsnapshot", rf.me, server)
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// Started as a separate goroutine at start, detects timeout
func (rf *Raft) detectTimeOutThread() {
	for {
		if rf.killed {
			break
		}
		timeOut := time.Duration(rand.Intn(timeOut)+timeOut) * time.Millisecond
		select {
		case <-time.After(timeOut):
			rf.mu.Lock()
			if rf.currentStatus != leader {
				rf.turnCandidate()
			}
			rf.mu.Unlock()
		case <-rf.updateTimeoutCh:
		}
	}
}

// Called to prevent timeout occuring, is non-blocking
func (rf *Raft) updateTimeout() {
	select {
	case rf.updateTimeoutCh <- struct{}{}:
	default:
	}
}

// All turn... functions should be called when holding lock
func (rf *Raft) turnCandidate() {
	DPrintf("Server %d has become candidate on Term %d", rf.me, rf.currentTerm)
	rf.currentStatus = candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.updateTimeout()
	rf.requestVotes()
}

func (rf *Raft) turnFollower(term int) {
	DPrintf("Server %d has become follower on Term %d", rf.me, rf.currentTerm)
	if rf.currentStatus == leader {
		select {
		case rf.leaderChangeCh <- struct{}{}:
		default:
		}
	}
	rf.currentStatus = follower
	rf.currentTerm = term
	rf.votedFor = -1
}

func (rf *Raft) turnLeader() {
	DPrintf("Server %d has become leader on Term %d", rf.me, rf.currentTerm)
	rf.currentStatus = leader
	rf.matchIndex = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))
	// rf.log = append(rf.log, Log{nil, rf.currentTerm, false})
	for i := 0; i < len(rf.peers); i++ {
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = len(rf.log) + rf.snapshotIndex
	}
	rf.appendEntries()
}

func (rf *Raft) requestVotes() {
	thisTerm := rf.currentTerm
	voteCount := 1
	lastLogIndex := len(rf.log) + rf.snapshotIndex - 1
	lastLogTerm := rf.log[len(rf.log)-1].Term
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			server := i
			args := RequestVoteArgs{
				Term:         thisTerm,
				CandidateId:  rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			go func() {
				var reply RequestVoteReply
				ok := rf.sendRequestVote(server, &args, &reply)
				if !ok {
					return
				}

				rf.mu.Lock()
				defer rf.mu.Unlock()
				// defer rf.persist()
				if reply.Term > rf.currentTerm {
					rf.turnFollower(reply.Term)
				}
				if rf.currentStatus != candidate || rf.currentTerm != thisTerm {
					return
				}
				if reply.VoteGranted {
					voteCount++
				}
				if voteCount > len(rf.peers)/2 {
					rf.turnLeader()
				}
			}()
		}
	}
}

// Started as separate goroutine at start, won't hold lock for long
// will try to apply commited log when `tryWakeApply()` is called
// `rf.wakeapplyCh` is a buffered channel, so it is guaranteed no to miss updates
// even if called multiple times when running one round
func (rf *Raft) tryApplyThread() {
	for {
		if rf.killed {
			break
		}
		// this is safe because commitIndex only increments, and it doesn't matter if
		// a race occurs. wakeApply will hold the notification
		thisCommitIndex := rf.commitIndex
		for i := rf.lastApplied + 1; i <= thisCommitIndex; i++ {
			// required because the possibility of snapshot occuring
			rf.mu.Lock()
			assert(i > rf.snapshotIndex)
			applyMsg := ApplyMsg{
				CommandValid: true,
				CommandIndex: i,
				Command:      rf.log[i-rf.snapshotIndex].Command,
			}
			DPrintf("applying %v", i)
			rf.mu.Unlock()
			rf.applyCh <- applyMsg
		}
		rf.lastApplied = thisCommitIndex
		<-rf.wakeApplyCh
	}
}

// Wakes the apply thread. Is non-blocking.
func (rf *Raft) tryWakeApply() {
	select {
	case rf.wakeApplyCh <- struct{}{}:
	default:
	}
}

func (rf *Raft) GetLeaderChangeCh() chan struct{} {
	return rf.leaderChangeCh
}

func (rf *Raft) GetRaftStateSize() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return len(rf.persister.ReadRaftState())
}
