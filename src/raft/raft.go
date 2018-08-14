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
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "labgob"

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
}

type Log struct {
	Command interface{}
	Term    int
}

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

	// utils
	currentStatus      status
	lastTimeFromLeader time.Time
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

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

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
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
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
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.lastTimeFromLeader = time.Now()

	if rf.currentTerm < args.Term {
		rf.votedFor = -1
		rf.currentTerm = args.Term
		rf.currentStatus = follower
		DPrintf("Server %d has become follower on Term %d", rf.me, rf.currentTerm)
	}

	if rf.currentStatus != follower {
		assert(rf.currentTerm == args.Term)
		assert(rf.currentStatus == candidate)
		rf.currentTerm = args.Term
		rf.currentStatus = follower
		DPrintf("Server %d has become follower on Term %d", rf.me, rf.currentTerm)
	}

	if len(rf.log) <= args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// delete conflicting entries...

	// append entries...

	// update commit
	if args.LeaderCommit > rf.commitIndex {
		lastIndex := len(rf.log) - 1
		rf.commitIndex = min(lastIndex, args.LeaderCommit)
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
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if rf.currentTerm < args.Term {
		rf.votedFor = -1
		rf.currentTerm = args.Term
		rf.currentStatus = follower
		DPrintf("Server %d has become follower on Term %d", rf.me, rf.currentTerm)
	}

	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term
	// Candidate at least as up-to-date
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(lastLogTerm < args.LastLogTerm ||
			(lastLogTerm == args.LastLogTerm && lastLogIndex <= args.LastLogIndex)) {
		rf.lastTimeFromLeader = time.Now()
		rf.votedFor = args.CandidateId
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
	} else {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentStatus == leader {
		term = rf.currentTerm
		index = len(rf.log)
		isLeader = true
		rf.log = append(rf.log, Log{ command, rf.currentTerm })
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
	// Your code here, if desired.
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
	rf.currentTerm = 1
	rf.currentStatus = follower
	rf.lastTimeFromLeader = time.Now()
	rf.log = make([]Log, 1)
	rf.commitIndex = 0

	// Your initialization code here (2A, 2B, 2C).
	// Watch for timeout here. If timeout, start a another term
	DPrintf("server %d start", me)
	go rf.run()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func (rf *Raft) run() {
	go rf.sendHeartBeats()
	go rf.detectTimeOut()
}

func (rf *Raft) sendHeartBeats() {
	for {
		time.Sleep(time.Duration(50) * time.Millisecond)
		rf.mu.Lock()
		if rf.currentStatus == leader {
			rf.appendEntries()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) appendEntries() {
	thisTerm := rf.currentTerm
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			server := i
			prevLogIndex := rf.nextIndex[server] - 1
			var entries []Log
			args := AppendEntriesArgs{
				Term:         thisTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  rf.log[prevLogIndex].Term,
				Entries:      entries,
				LeaderCommit: rf.commitIndex,
			}

			go func() {
				var reply AppendEntriesReply
				// set entries according to prevLogIndex
				ok := rf.sendAppendEntries(server, &args, &reply)
				if !ok {
					return
				}

				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.currentTerm != thisTerm {
					return
				}
				if reply.Success {
					rf.matchIndex[server] = max(rf.nextIndex[server]+len(entries)-1, rf.matchIndex[server])
					rf.nextIndex[server] = max(rf.nextIndex[server]+len(entries), rf.nextIndex[server])
				} else {
					if thisTerm < reply.Term {
						rf.currentTerm = reply.Term
						rf.currentStatus = follower
						DPrintf("Server %d has become follower on Term %d", rf.me, rf.currentTerm)
					} else {
						// append fail, decrease
					}
				}
			}()

		}
	}
}

func (rf *Raft) detectTimeOut() {
	for {
		time.Sleep(time.Duration(50) * time.Millisecond)
		rf.mu.Lock()
		diffTime := time.Now().Sub(rf.lastTimeFromLeader)
		followerTimeOut := time.Duration(rand.Intn(100)+200) * time.Millisecond
		candidateTimeOut := time.Duration(rand.Intn(100)+400) * time.Millisecond
		if (rf.currentStatus == follower && diffTime > followerTimeOut) ||
			(rf.currentStatus == candidate && diffTime > candidateTimeOut) {
			rf.currentStatus = candidate
			DPrintf("Server %d has become candidate on Term %d", rf.me, rf.currentTerm)
			rf.currentTerm++
			rf.votedFor = rf.me
			rf.requestVotes()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) requestVotes() {
	thisTerm := rf.currentTerm
	voteCount := 1
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term
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
				if rf.currentStatus != candidate || rf.currentTerm != thisTerm {
					return
				}
				if reply.VoteGranted {
					voteCount++
				}
				if voteCount > len(rf.peers)/2 {
					DPrintf("Server %d has become leader on Term %d", rf.me, rf.currentTerm)
					rf.currentStatus = leader
					rf.matchIndex = make([]int, len(rf.peers))
					rf.nextIndex = make([]int, len(rf.peers))
					for i := 0; i < len(rf.peers); i++ {
						rf.matchIndex[i] = 0
						rf.nextIndex[i] = len(rf.log)
					}
				}
			}()
		}
	}
	rf.lastTimeFromLeader = time.Now()
}
