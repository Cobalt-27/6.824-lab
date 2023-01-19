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
	//	"bytes"
	"bytes"
	"fmt"
	"math/rand"
	// "runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"
	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

const electionTimeout int64 = 300

var printVerbose bool = false
var printInfo bool = true

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
const (
	follower  = 0
	candidate = 1
	leader    = 2
)

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	applyChan     chan ApplyMsg
	lastHeartBeat int64
	role          int
	currentTerm   int    //persist
	votedFor      int    //persist
	lastVoteTerm  int    //persist
	snapshotIndex int    //persist
	snapshotTerm  int    //persist
	snapshot      []byte //persist*

	log         []interface{} //persist
	logTerm     []int         //persist
	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == leader
}

func (rf *Raft) majority() int {
	return rf.peerCount()/2 + 1
}

func (rf *Raft) peerCount() int {
	return len(rf.peers)
}

func (rf *Raft) size() int {
	return len(rf.log) + rf.snapshotIndex
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
	e.Encode(rf.logTerm)
	e.Encode(rf.lastVoteTerm)
	e.Encode(rf.snapshotIndex)
	e.Encode(rf.snapshotTerm)
	data := w.Bytes()
	// rf.persister.SaveRaftState((data))
	rf.persister.SaveStateAndSnapshot(data, rf.snapshot)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	currentTerm := 0
	votedFor := 0
	log := make([]interface{}, 0)
	logTerm := make([]int, 0)
	lastVoteTerm := 0
	snapshotIndex := 0
	snapshotTerm := 0
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&logTerm) != nil ||
		d.Decode(&lastVoteTerm) != nil ||
		d.Decode(&snapshotIndex) != nil ||
		d.Decode(&snapshotTerm) != nil {
		panic(1)
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.logTerm = logTerm
		rf.lastVoteTerm = lastVoteTerm
		rf.snapshotIndex = snapshotIndex
		rf.snapshotTerm = snapshotTerm
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.verbose("SNAPSHOT at %d %v", index, snapshot)
	if index < 1 {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	trimLen := index - rf.snapshotIndex

	rf.snapshotTerm = rf.getLogTerm(index)
	rf.snapshotIndex = index //set snapshotTerm first

	rf.snapshot = snapshot
	rf.log = rf.log[trimLen:]
	rf.logTerm = rf.logTerm[trimLen:]
	rf.persist()
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastlogIndex int
	LastLogTerm  int
	// Your data here (2A, 2B).
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
	// Your data here (2A).
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.persist()
		rf.role = follower
	}
	voteTrue := func() {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.lastVoteTerm = args.Term
		rf.persist()
		rf.verbose("vote true to %d", args.CandidateId)
	}

	voteFalse := func() {
		reply.VoteGranted = false
		rf.verbose("vote false to %d lastVote=%d votedFor=%d logT=%d logI=%d myLogT=%d myLogI=%d", args.CandidateId, rf.lastVoteTerm, rf.votedFor, args.LastLogTerm, args.LastlogIndex, rf.lastLogTerm(), rf.lastLogIndex())
	}

	reply.Term = rf.currentTerm
	if (rf.lastVoteTerm < args.Term || rf.votedFor == args.CandidateId) && rf.currentTerm <= args.Term {
		if rf.isUpToDate(args.LastlogIndex, args.LastLogTerm) {
			voteTrue()
			return
		} else {
			voteFalse()
			return
		}
	} else {
		voteFalse()
	}
	// Your code here (2A, 2B).
}

type LogEntry struct {
	EntryTerm int
	EntryVal  interface{}
}

type AppendEntriesArgs struct {
	Term         int
	LearderId    int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term              int
	Success           bool
	ConflictTerm      int
	ConflictTermBegin int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.persist()
		rf.role = follower
	}
	if args.Term < rf.currentTerm {
		reply.Success = false
		rf.verbose("AppendEntries: staled term %d", args.Term)
		return
	}
	if args.PrevLogIndex < rf.snapshotIndex {
		reply.Success = false
		rf.verbose("AppendEntries: log discarded %d", args.Term)
		return
	}
	prevCorrect := func(prevIndex int, prevTerm int) bool {
		if args.PrevLogIndex > rf.lastLogIndex() { //index do not exist
			return false
		}
		if rf.getLogTerm(args.PrevLogIndex) != args.PrevLogTerm {
			return false
		} else {
			return true
		}
	}

	min := func(a int, b int) int {
		if a < b {
			return a
		} else {
			return b
		}
	}

	//up to date
	if len(args.Entries) == 0 { //heartbeat
		rf.verbose("recv heartbeat from %d", args.LearderId)
		rf.lastHeartBeat = time.Now().UnixMilli()
		rf.currentTerm = args.Term
		rf.persist()
		rf.role = follower
		reply.Term = rf.currentTerm
		if prevCorrect(args.PrevLogIndex, args.PrevLogTerm) {
			rf.commitIndex = min(args.LeaderCommit, rf.lastLogIndex())
			reply.Success = true
			rf.verbose("heartbeat: COMMIT=%d", rf.commitIndex)
			return
		} else {
			rf.verbose("APPEND FAIL(empty log) prevLogIndex=%v prevLogTerm=%v", args.PrevLogIndex, args.PrevLogTerm)
			reply.Success = false
			reply.ConflictTerm = -1
			reply.ConflictTermBegin = -1
			return
		}
	} else {
		if !prevCorrect(args.PrevLogIndex, args.PrevLogTerm) {
			rf.verbose("APPEND FAIL prevLogIndex=%v prevLogTerm=%v", args.PrevLogIndex, args.PrevLogTerm)
			reply.Success = false
			if args.PrevLogIndex < rf.lastLogIndex() {
				reply.ConflictTerm = rf.getLogTerm(args.PrevLogIndex)
				idx := args.PrevLogIndex
				for {
					if idx > 1 && idx > rf.snapshotIndex && rf.getLogTerm(idx-1) == reply.ConflictTerm {
						idx--
					} else {
						break
					}
				}
				reply.ConflictTermBegin = idx
			} else {
				reply.ConflictTerm = -1
				reply.ConflictTermBegin = -1
			}
			return
		} else {
			start := args.PrevLogIndex + 1
			rf.AddEntries(args.Entries, start)
			rf.commitIndex = min(args.LeaderCommit, rf.lastLogIndex())
			rf.verbose("LOG+=%d", len(args.Entries))
			rf.verbose("COMMIT=%d", rf.commitIndex)
			reply.Success = true
			return
		}
	}

}

type InstallSnapshotArgs struct {
	Term int
	// leaderId int
	LastIncludedIndex int
	LastIncludedTerm  int
	Snapshot          []byte
}

type InstallSnapshotReply struct {
	Success bool
	Term    int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.verbose("INSTALL %d:%d len=%d", args.LastIncludedIndex, args.LastIncludedTerm, len(args.Snapshot))
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		rf.verbose("INSTALL fail: stale packet")
		return
	}
	if args.LastIncludedIndex < rf.snapshotIndex {
		reply.Success = false
		rf.verbose("INSTALL fail: stale index")
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = follower
	}
	discardLog := false
	if args.LastIncludedIndex >= rf.lastLogIndex() {
		discardLog = true
	} else if args.LastIncludedTerm != rf.getLogTerm(args.LastIncludedIndex) {
		discardLog = true
	}

	if discardLog {
		rf.log = make([]interface{}, 0)
		rf.logTerm = make([]int, 0)
	} else {
		trimLen := args.LastIncludedIndex - rf.snapshotIndex
		rf.log = rf.log[trimLen:]
		rf.logTerm = rf.logTerm[trimLen:]
	}
	reply.Success = true
	rf.snapshotIndex = args.LastIncludedIndex
	rf.snapshotTerm = args.LastIncludedTerm
	rf.snapshot = args.Snapshot
	rf.persist()
}

func (rf *Raft) verbose(format string, a ...interface{}) { //may produce race
	if printVerbose {
		header := fmt.Sprintf("[%d,%d,%d]", rf.me, rf.currentTerm, rf.role)
		fmt.Printf(header+format+"\n", a...)
	}
}

func (rf *Raft) info(format string, a ...interface{}) { //may produce race
	if printInfo {
		header := fmt.Sprintf("[%d,%d,%d] ", rf.me, rf.currentTerm, rf.role)
		fmt.Printf(header+format+"\n", a...)
	}
}

func (rf *Raft) isUpToDate(lastLogIndex int, lastLogTerm int) bool {
	return lastLogTerm > rf.lastLogTerm() || (lastLogTerm == rf.lastLogTerm() && lastLogIndex >= rf.lastLogIndex())
}

func (rf *Raft) sleep(milliSecond int64) {
	time.Sleep(time.Duration(milliSecond * int64(time.Millisecond)))
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
	// rf.debug("RequestVote sent to%d", server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	// if len(args.Entries) != 0 {
	// 	rf.debug("AppendEntries sent to %d len=%d", server, len(args.Entries))
	// }
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

//log[i] for i in [start,end]
func (rf *Raft) makeEntries(start int, end int) []LogEntry {
	len := end - start + 1
	entries := make([]LogEntry, len)
	for i := 0; i < len; i++ {
		entries[i] = LogEntry{
			EntryTerm: rf.getLogTerm(i + start),
			EntryVal:  rf.getLog(i + start),
		}
	}
	return entries
}

func (rf *Raft) syncLog() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != leader || rf.size() == 0 {
		return
	}
	for remote := range rf.peers {
		if remote == rf.me {
			continue
		}
		next := rf.nextIndex[remote]
		if next <= rf.lastLogIndex() {
			rf.verbose("sync range=%d-%d to %d", next, rf.lastLogIndex(), remote)
			prev := next - 1
			if prev < rf.snapshotIndex { //log entry discarded
				rf.verbose("sync via snapshot %d<%d", prev, rf.snapshotIndex)
				snapshotCopy := make([]byte, len(rf.snapshot))
				copy(snapshotCopy, rf.snapshot)
				args := InstallSnapshotArgs{
					Term:              rf.currentTerm,
					LastIncludedIndex: rf.snapshotIndex,
					LastIncludedTerm:  rf.snapshotTerm,
					Snapshot:          snapshotCopy,
				}
				replyChan := make(chan InstallSnapshotReply)
				go func(to int, args InstallSnapshotArgs) {
					reply := InstallSnapshotReply{}
					rf.sendInstallSnapshot(to, &args, &reply)
					replyChan <- reply
				}(remote, args)
				go func(from int, successNext int, term int) {
					reply := <-replyChan
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if rf.currentTerm != term {
						return
					}
					rf.verbose("recv install %v from %d", reply.Success, from)
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.persist()
						rf.role = follower
						go rf.startElection(rf.currentTerm)
					}
					if reply.Success {
						rf.nextIndex[from] = successNext
						rf.matchIndex[from] = successNext - 1
					}
				}(remote, rf.snapshotIndex+1, rf.currentTerm)
			} else {
				prevTerm := rf.getLogTerm(prev)
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LearderId:    rf.me,
					PrevLogIndex: prev,
					PrevLogTerm:  prevTerm,
					Entries:      rf.makeEntries(next, rf.lastLogIndex()),
					LeaderCommit: rf.commitIndex,
				}
				replyChan := make(chan AppendEntriesReply)
				go func(to int, args AppendEntriesArgs) {
					reply := AppendEntriesReply{}
					rf.sendAppendEntries(to, &args, &reply)
					replyChan <- reply
				}(remote, args)
				go func(from int, successNext int, term int) {
					reply := <-replyChan
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if rf.currentTerm != term {
						return
					}
					rf.verbose("recv AppendEntries reply %v from %v", reply.Success, from)
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.persist()
						rf.role = follower
						go rf.startElection(rf.currentTerm)
					}
					if reply.Success {
						rf.nextIndex[from] = successNext
						rf.matchIndex[from] = successNext - 1
					} else {
						prev := rf.nextIndex[from] - 2
						if reply.ConflictTerm != -1 && reply.ConflictTermBegin != -1 {
							for {
								if prev > rf.snapshotIndex && rf.getLogTerm(prev) != reply.ConflictTerm && prev >= reply.ConflictTermBegin {
									prev--
								} else {
									break
								}
							}
						}
						rf.nextIndex[from] = prev + 1
						if rf.nextIndex[from] < 1 {
							rf.nextIndex[from] = 1
						}
					}
				}(remote, rf.lastLogIndex()+1, rf.currentTerm)
			}
		}
	}
}

func (rf *Raft) updateCommit() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != leader || rf.size() == 0 {
		return
	}
	rf.matchIndex[rf.me] = rf.lastLogIndex()
	match := make([]int, rf.peerCount())
	copy(match, rf.matchIndex)
	sort.Ints(match)
	rf.verbose("match: %v", match)
	newCommit := match[len(match)-rf.majority()]
	if rf.getLogTerm(newCommit) == rf.currentTerm && rf.commitIndex < newCommit {
		rf.commitIndex = newCommit
		rf.verbose("COMMIT=%d", newCommit)
	}
}

func (rf *Raft) checkApply() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.lastApplied < rf.commitIndex {
		applied := rf.lastApplied + 1
		if applied < 1 {
			applied = 1
		}
		if applied <= rf.snapshotIndex {
			rf.verbose("APPLY SNAPSHOT %d:%d", rf.snapshotIndex, rf.snapshotTerm)
			snapshotCopy := make([]byte, len(rf.snapshot))
			copy(snapshotCopy, rf.snapshot)
			msg := ApplyMsg{
				SnapshotValid: true,
				Snapshot:      snapshotCopy,
				SnapshotIndex: rf.snapshotIndex,
				SnapshotTerm:  rf.snapshotTerm,
			}
			rf.mu.Unlock()
			rf.applyChan <- msg
			rf.mu.Lock()
			rf.verbose("APPLYMSG sent SNAPSHOT")
			rf.lastApplied = rf.snapshotIndex
			return true
		} else {
			rf.verbose("APPLY %d", applied)
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.getLog(applied),
				CommandIndex: applied,
			}
			rf.mu.Unlock()
			rf.applyChan <- msg
			rf.mu.Lock()
			rf.verbose("APPLYMSG SENT idx=%d", applied)
			rf.lastApplied = applied
			return true
		}
	} else {
		return false
	}
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
	// if command == 111111 {
	// 	printVerbose = true
	// }
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.verbose("START new agreement %v", command)
	index := -1
	term := -1
	isLeader := rf.role == leader
	if isLeader {
		index = rf.lastLogIndex() + 1
		term = rf.currentTerm
		rf.log = append(rf.log, command)
		rf.logTerm = append(rf.logTerm, term)
		rf.persist()
	}

	// Your code here (2B).
	rf.verbose("START return: index=%v term=%v isLeader=%v", index, term, isLeader)

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	tickerSleep := func() {
		rf.sleep(rand.Int63n(electionTimeout) + electionTimeout)
	}
	tickerSleep()
	for !rf.killed() {
		// rf.debug("tick")
		rf.mu.Lock()
		if rf.role != leader && (time.Now().UnixMilli()-rf.lastHeartBeat) > electionTimeout { //timeout
			// rf.role = candidate
			term := rf.currentTerm
			rf.mu.Unlock()
			rf.startElection(term)
		} else {
			//
			rf.mu.Unlock()
		}
		tickerSleep()
	}
}

func (rf *Raft) heartbeat() {
	for !rf.killed() {
		rf.mu.Lock()
		rf.sendHeartbeats()
		rf.mu.Unlock()
		rf.sleep(electionTimeout / 2)
	}
}

func (rf *Raft) sendHeartbeats() {
	if rf.role == leader {
		reply := AppendEntriesReply{}
		for remote := range rf.peers {
			if remote == rf.me {
				rf.lastHeartBeat = time.Now().UnixMilli()
				continue
			}
			args := AppendEntriesArgs{
				LearderId:    rf.me,
				Term:         rf.currentTerm,
				PrevLogIndex: rf.lastLogIndex(),
				PrevLogTerm:  rf.getLogTerm(rf.lastLogIndex()),
				LeaderCommit: rf.commitIndex,
			}
			go func(to int, args AppendEntriesArgs, reply AppendEntriesReply) {
				rf.sendAppendEntries(to, &args, &reply)
			}(remote, args, reply)
		}
	}
}

//External LastLogIndex, -1 for empty log. Thread unsafe.
func (rf *Raft) lastLogIndex() int {
	return rf.size()
}

func (rf *Raft) getLog(index int) interface{} {
	if index == 0 {
		return nil
	}
	return rf.log[index-rf.snapshotIndex-1]
}

func (rf *Raft) AddEntries(entries []LogEntry, startIndex int) {
	addCount := len(entries)
	newLog := make([]interface{}, addCount)
	newLogTerm := make([]int, addCount)
	for i := range entries {
		newLog[i] = entries[i].EntryVal
		newLogTerm[i] = entries[i].EntryTerm
	}
	fixedIndex := startIndex - rf.snapshotIndex
	rf.log = append(rf.log[:fixedIndex-1], newLog...)
	rf.logTerm = append(rf.logTerm[:fixedIndex-1], newLogTerm...)
	rf.persist()
}

func (rf *Raft) getLogTerm(index int) int {
	if index == 0 {
		return 0
	}
	if index == rf.snapshotIndex {
		return rf.snapshotTerm
	}
	return rf.logTerm[index-rf.snapshotIndex-1]
}

//External LastLogTerm, 0 for empty log. Thread unsafe.
func (rf *Raft) lastLogTerm() int {
	idx := rf.lastLogIndex()
	if idx < 1 {
		return 0
	}
	return rf.getLogTerm(idx)
}

//Start Election.\
func (rf *Raft) startElection(oldTerm int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if oldTerm != rf.currentTerm {
		return
	}
	rf.currentTerm++

	startTerm := rf.currentTerm
	rf.votedFor = rf.me
	rf.lastVoteTerm = rf.currentTerm
	rf.persist()
	rf.role = candidate

	rf.verbose("start election\n")

	voteChan := make(chan RequestVoteReply, rf.peerCount()*2)
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastlogIndex: rf.lastLogIndex(),
		LastLogTerm:  rf.lastLogTerm(),
	}

	for remote := range rf.peers {
		if remote == rf.me {
			continue
		}
		// rf.info("go sendRequestVote")
		go func(peer int, args RequestVoteArgs) {
			reply := RequestVoteReply{
				VoteGranted: false,
			}
			rf.sendRequestVote(peer, &args, &reply)
			voteChan <- reply
		}(remote, args)
	}
	// go func(start int64) {
	// 	for {
	// 		if time.Now().UnixMilli()-start > electionTimeout*3 {
	// 			for i := 0; i < rf.peerCount(); i++ {
	// 				voteChan <- RequestVoteReply{
	// 					VoteGranted: false,
	// 				}
	// 			}
	// 		}
	// 		rf.sleep(electionTimeout / 2)
	// 	}
	// }(time.Now().UnixMilli())

	granted := 1 //1 vote from self
	deny := 0
	winCount := rf.majority()
	loseCount := rf.peerCount() - winCount + 1
	for i := 0; i < rf.peerCount()-1; i++ {
		rf.verbose("waiting for vote")
		rf.mu.Unlock()
		res := <-voteChan
		rf.mu.Lock()
		rf.verbose("get vote %t (%d)", res.VoteGranted, rf.peerCount())
		if rf.role != candidate || rf.currentTerm != startTerm {
			return
		}
		if res.VoteGranted {
			granted++
		} else {
			deny++
		}
		if res.Term > rf.currentTerm {
			rf.verbose("candidate step down reply.term=%d myterm=%d", res.Term, rf.currentTerm)
			rf.role = follower
			rf.currentTerm = res.Term
			rf.persist()
		}
		if granted >= winCount {
			//now leader
			rf.verbose("becomes leader")
			rf.role = leader
			rf.nextIndex = make([]int, rf.peerCount())
			for remote := range rf.peers {
				rf.nextIndex[remote] = rf.lastLogIndex() + 1
			}
			rf.matchIndex = make([]int, rf.peerCount())
			rf.sendHeartbeats()
			return
		}
		if deny >= loseCount {
			rf.verbose("failed election (%d- vs %d+)", deny, granted)
			rf.role = follower
			return
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
	// fmt.Printf("gonum=%v\n", runtime.NumGoroutine())
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.applyChan = applyCh
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.role = follower
	// Your initialization code here (2A, 2B, 2C).
	rf.lastHeartBeat = time.Now().UnixMilli()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.Snapshot(rf.snapshotIndex, persister.ReadSnapshot())
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.heartbeat()
	go func() {
		for !rf.killed() {
			rf.syncLog()
			rf.updateCommit()
			for rf.checkApply() {
			}
			rf.sleep(electionTimeout / 3)
		}
	}()
	return rf
}
