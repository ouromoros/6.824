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
	"log"
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
	applyCh   chan ApplyMsg

	// utils
	currentStatus status
	// timeOutSeqNum int
	wakeApplyCh   chan struct{}
	updateTimeoutCh chan struct{}
	// lastTimeFromLeader time.Time
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

	debug  int
	killed bool
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	rf.persister.SaveRaftState(w.Bytes())
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []Log
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		panic("Decode Failed!")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
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

	// rf.lastTimeFromLeader = time.Now()
	// go rf.updateAndSetTimeout()
	rf.updateTimeout()

	if rf.currentTerm < args.Term {
		rf.turnFollower(args.Term)
	}

	if rf.currentStatus != follower {
		assert(rf.currentTerm == args.Term)
		assert(rf.currentStatus == candidate)
		rf.turnFollower(args.Term)
	}

	if len(rf.log) <= args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		if len(rf.log)-1 <= args.PrevLogIndex {
			reply.FailTerm = rf.log[len(rf.log)-1].Term
		} else {
			reply.FailTerm = rf.log[args.PrevLogIndex].Term
		}
		return
	}

	var match int
	// delete conflicting entries...
	for match = 0; match < len(rf.log)-args.PrevLogIndex-1 && match < len(args.Entries); match++ {
		if rf.log[args.PrevLogIndex+match+1].Term != args.Entries[match].Term {
			rf.log = rf.log[:args.PrevLogIndex+match+1]
			break
		}
	}
	// append entries... might have matched all
	if match < len(args.Entries) {
		rf.log = append(rf.log, args.Entries[match:]...)
	}

	// update commit
	if args.LeaderCommit > rf.commitIndex {
		lastIndex := len(rf.log) - 1
		rf.commitIndex = min(lastIndex, args.LeaderCommit)
		rf.tryWakeApply()
	}
	reply.Term = rf.currentTerm
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	// rf.DPrintf("Server %d sending ap to %d", rf.me, server)
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

	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term
	// Candidate at least as up-to-date
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(lastLogTerm < args.LastLogTerm ||
			(lastLogTerm == args.LastLogTerm && lastLogIndex <= args.LastLogIndex)) {
		// rf.lastTimeFromLeader = time.Now()
		// go rf.updateAndSetTimeout()
		rf.updateTimeout()
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
	// rf.DPrintf("Server %d sending rv to %d", rf.me, server)
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
	index := len(rf.log)
	if rf.currentStatus == leader {
		isLeader = true
		rf.log = append(rf.log, Log{command, rf.currentTerm})
		rf.persist()
		rf.DPrintf("Server %d recevied client command", rf.me)
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
	rf.mu.Lock()
	rf.debug = 0
	rf.mu.Unlock()
}

func (rf *Raft) DPrintf(format string, a ...interface{}) (n int, err error) {
	if rf.debug > 0 {
		log.Printf(format, a...)
	}
	return
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
	rf.currentTerm = 1
	rf.currentStatus = follower
	// rf.lastTimeFromLeader = time.Now().Add(time.Duration(-900) * time.Millisecond)
	rf.log = make([]Log, 1)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.wakeApplyCh = make(chan struct{}, 1)
	rf.updateTimeoutCh = make(chan struct{}, 1)

	rf.killed = false
	rf.debug = Debug

	// Your initialization code here (2A, 2B, 2C).
	// Watch for timeout here. If timeout, start a another term
	rf.DPrintf("server %d start", me)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.run()

	return rf
}

func (rf *Raft) run() {
	go rf.sendHeartBeatsThread()
	go rf.tryApplyThread()
	go rf.detectTimeOutThread()
	// go rf.updateAndSetTimeout()
	// go rf.detectTimeOut()
}

func (rf *Raft) sendHeartBeatsThread() {
	for {
		if rf.killed {
			break
		}
		time.Sleep(time.Duration(100) * time.Millisecond)
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
			prevLogTerm := rf.log[prevLogIndex].Term
			entriesLen := len(rf.log) - rf.nextIndex[server]
			entries := make([]Log, entriesLen)
			copy(entries, rf.log[rf.nextIndex[server]:])
			thisNextIndex := rf.nextIndex[server]
			thisMatchIndex := rf.matchIndex[server]
			commitIndex := rf.commitIndex
			assert(prevLogIndex < len(rf.log))
			assert(prevLogIndex >= 0)
			args := AppendEntriesArgs{
				Term:         thisTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: commitIndex,
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
				if reply.Term > rf.currentTerm {
					rf.turnFollower(reply.Term)
					return
				}
				if rf.currentTerm != thisTerm {
					return
				}
				if reply.Success {
					rf.matchIndex[server] = max(thisNextIndex+len(entries)-1, rf.matchIndex[server])
					rf.nextIndex[server] = max(thisNextIndex+len(entries), rf.nextIndex[server])
					for index := rf.commitIndex + 1; index < len(rf.log); index++ {
						if rf.log[index].Term == rf.currentTerm {
							matchCount := 1
							for i := 0; i < len(rf.peers); i++ {
								if i != rf.me && rf.matchIndex[i] >= index {
									matchCount++
								}
							}
							if matchCount > len(rf.peers)/2 {
								rf.commitIndex = index
								rf.DPrintf("Server %d: commit index updated to %d", rf.me, rf.commitIndex)
							} else {
								break
							}
						}
					}
					// rf.DPrintf("Server %d: client %d success on %d", rf.me, server, thisMatchIndex+len(entries))
					rf.tryWakeApply()
				} else {
					if thisTerm < reply.Term {
						rf.turnFollower(reply.Term)
					} else {
						if rf.nextIndex[server] != thisNextIndex || rf.matchIndex[server] != thisMatchIndex {
							return
						}
						if reply.FailTerm < args.PrevLogTerm {
							for rf.log[rf.nextIndex[server]-1].Term > reply.FailTerm {
								rf.nextIndex[server]--
							}
						} else {
							rf.nextIndex[server] = min(rf.nextIndex[server], prevLogIndex)
						}
						rf.DPrintf("Server %d: client %d fail on %d", rf.me, server, prevLogIndex)
						rf.appendEntries()
						assert(rf.nextIndex[server] > 0)
					}
				}
			}()

		}
	}
}

// func (rf *Raft) detectTimeOut() {
// 	rand.Seed(int64(time.Now().Nanosecond()))
// 	timeOut := time.Duration(rand.Intn(1000)+1000) * time.Millisecond
// 	for {
// 		time.Sleep(time.Duration(10) * time.Millisecond)
// 		rf.mu.Lock()
// 		diffTime :=  time.Now().Sub(rf.lastTimeFromLeader)
// 		if rf.currentStatus != leader && diffTime > timeOut {
// 			timeOut = time.Duration(rand.Intn(1000)+1000) * time.Millisecond
// 			rf.currentStatus = candidate
// 			rf.currentTerm++
// 			rf.votedFor = rf.me
// 			rf.lastTimeFromLeader = time.Now()
// 			rf.DPrintf("Server %d has become candidate on Term %d", rf.me, rf.currentTerm)
// 			rf.requestVotes()
// 		}
// 		rf.persist()
// 		rf.mu.Unlock()
// 	}
// }

// func (rf *Raft) updateAndSetTimeout() {
// 	timeOut := time.Duration(rand.Intn(1000)+1000) * time.Millisecond
// 	rf.mu.Lock()
// 	rf.timeOutSeqNum++
// 	seq := rf.timeOutSeqNum
// 	rf.mu.Unlock()

// 	time.Sleep(timeOut)

// 	rf.mu.Lock()
// 	if rf.currentStatus != leader && rf.timeOutSeqNum == seq {
// 		rf.turnCandidate()
// 	}
// 	rf.mu.Unlock()
// }

func (rf *Raft) detectTimeOutThread() {
	for {
		timeOut := time.Duration(rand.Intn(1000)+1000) * time.Millisecond
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

func (rf *Raft) updateTimeout() {
	select {
	case rf.updateTimeoutCh <- struct{}{}:
	default:
	}
}

func (rf *Raft) turnCandidate() {
	rf.DPrintf("Server %d has become candidate on Term %d", rf.me, rf.currentTerm)
	rf.currentStatus = candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	// rf.lastTimeFromLeader = time.Now()
	// go rf.updateAndSetTimeout()
	rf.updateTimeout();
	rf.requestVotes()
}

func (rf *Raft) turnFollower(term int) {
	rf.DPrintf("Server %d has become follower on Term %d", rf.me, rf.currentTerm)
	rf.currentStatus = follower
	rf.currentTerm = term
	rf.votedFor = -1
}

func (rf *Raft) turnLeader() {
	rf.DPrintf("Server %d has become leader on Term %d", rf.me, rf.currentTerm)
	rf.currentStatus = leader
	rf.matchIndex = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = len(rf.log)
	}
	rf.appendEntries()
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
					rf.DPrintf("Server %d send rv fail!", rf.me)
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
					rf.DPrintf("Server %d has got vote from %d", rf.me, server)
					voteCount++
				}
				if voteCount > len(rf.peers)/2 {
					rf.turnLeader()
				}
			}()
		}
	}
}

func (rf *Raft) tryApplyThread() {
	for {
		if rf.killed { break }
		rf.mu.Lock()
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				CommandIndex: i,
				Command:      rf.log[i].Command,
			}
			// rf.DPrintf("Server %d: applied %d", rf.me, i)
		}
		rf.lastApplied = rf.commitIndex
		rf.mu.Unlock()
		<-rf.wakeApplyCh
	}
}

func (rf *Raft) tryWakeApply() {
	select {
	case rf.wakeApplyCh <- struct{}{}:
	default:
	}
}

// func (rf *Raft) tryApply() {
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()

// 	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
// 		rf.applyCh <- ApplyMsg{
// 			CommandValid: true,
// 			CommandIndex: i,
// 			Command:      rf.log[i].Command,
// 		}
// 	}
// 	rf.lastApplied = rf.commitIndex
// }
