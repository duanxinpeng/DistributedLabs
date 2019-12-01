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
	"log"
	"math/rand"
	"sync"
	"time"
)
import "labrpc"

const (
	Follower  int = 0
	Candidate int = 1
	Leader    int = 2
)

const ElectTimeout = time.Duration(1000 * time.Millisecond)
const AppendEntriesInterval = time.Duration(100 * time.Millisecond)

type Err string

const OK = "OK"
const ErrRPCFail = "ErrRPCFail"

// import "bytes"
// import "labgob"

type LogEntry struct {
	LogIndex int
	LogTerm  int
	Commond  interface{} //????????????
}

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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	//2A
	currentTerm int
	leaderID    int
	votedFor    int
	log         []LogEntry
	logIndex    int
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
	state       int //0 follower; 1 candidate; 2 leader;
	timer       *time.Timer
	shutdown    chan struct{}

	applyCh       chan ApplyMsg
	notifyApplyCh chan struct{}

	lastIncludeIndex int
}

func newRandDuration(minDuration time.Duration) time.Duration {
	extra := time.Duration(rand.Int63()) % minDuration
	return minDuration + extra
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//rf.leaderId==rf.me???
	return rf.currentTerm, rf.state == Leader
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
	data := rf.getPersistState()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) getPersistState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.lastIncludeIndex) //顺序
	e.Encode(rf.logIndex)
	e.Encode(rf.commitIndex)
	e.Encode(rf.lastApplied)
	e.Encode(rf.log)

	data := w.Bytes()
	return data
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
	currentTerm, votedFor, lastIncludedIndex, logIndex, commitIndex, lastApplied := 0, 0, 0, 0, 0, 0
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&logIndex) != nil ||
		d.Decode(&commitIndex) != nil ||
		d.Decode(&lastApplied) != nil ||
		d.Decode(&rf.log) != nil {
		log.Fatal("Error in unmarshal raft state")
	}
	rf.currentTerm, rf.votedFor, rf.lastIncludeIndex, rf.logIndex, rf.commitIndex, rf.lastApplied = currentTerm, votedFor, lastIncludedIndex, logIndex, commitIndex, lastApplied
}

//
func (rf *Raft) PersistAndSaveSnapshot(lastIncludedIndex int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if lastIncludedIndex > rf.lastIncludeIndex {
		truncationStartIndex := rf.getOffsetIndex(lastIncludedIndex)
		rf.log = append([]LogEntry{}, rf.log[truncationStartIndex:]...) // log entry previous at lastIncludedIndex at 0 now
		rf.lastIncludeIndex = lastIncludedIndex
		data := rf.getPersistState()
		rf.persister.SaveStateAndSnapshot(data, snapshot)
	}
}
func (rf *Raft) getOffsetIndex(i int) int {
	return i - rf.lastIncludeIndex
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
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PreLogIndex  int
	PreLogTerm   int
	Entries      []LogEntry
	LeaderCommit int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
	Server      int
	Err         Err
}
type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int //
}

//2C code
type InstallSnapshotArgs struct {
	Term             int
	LeaderId         int
	LastIncludeIndex int
	LastIncludeTerm  int
	Data             []byte
}
type InstallSnapshotReply struct {
	Err  Err
	Term int
}

//2A code
func (rf *Raft) resetElectionTimer(duration time.Duration) {
	rf.timer.Stop()
	rf.timer.Reset(duration)
}

//
// example RequestVote RPC handler.
//

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
//func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
//	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
//	return ok
//}
//func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply)bool{
//	ok:=rf.peers[server].Call("Raft.AppendEntries",args,reply)
//	return ok
//}

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
	if rf.state != Leader {
		return -1, -1, false
	}
	index = rf.logIndex
	term = rf.currentTerm
	entry := LogEntry{
		LogIndex: index,
		LogTerm:  term,
		Commond:  command,
	}
	rf.log = append(rf.log, entry)
	rf.matchIndex[rf.me] = rf.logIndex //
	rf.logIndex += 1                   //和nextIndex是有区别的
	go rf.replicate()
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
	defer rf.mu.Unlock()
	rf.state = Follower
	close(rf.shutdown) //??????????
}
func (rf *Raft) campaign() {
	rf.mu.Lock()
	if rf.state == Leader { //只有follower才需要 compaign
		rf.mu.Unlock()
		return
	}
	rf.currentTerm += 1
	rf.state = Candidate
	rf.leaderID = -1
	rf.votedFor = rf.me

	rf.persist() //????????????

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.logIndex - 1,
		LastLogTerm:  rf.log[rf.logIndex-1].LogTerm,
	}
	numServers := len(rf.peers)
	newElectionTime := newRandDuration(ElectTimeout)
	rf.resetElectionTimer(newElectionTime)
	timer := time.After(newElectionTime)
	replyCh := make(chan RequestVoteReply, numServers-1) //?????????????
	for i := 0; i < numServers; i++ {
		if i != rf.me {
			go rf.solicit(i, args, replyCh)
		}
	}
	rf.mu.Unlock()
	//
	voteCount, threshold := 0, numServers/2
	for voteCount < threshold {
		select {
		case <-rf.shutdown:
			return
		case <-timer:
			return //timer 和 rf.timer是两个线程同时在计时
		case reply := <-replyCh:
			if reply.Err != OK {
				go rf.solicit(reply.Server, args, replyCh)
			} else if reply.VoteGranted {
				voteCount += 1
			} else {
				rf.mu.Lock()
				if rf.currentTerm < reply.Term {
					rf.stepDown(reply.Term) // ？？？？？？？？？？？？？？？？？？？？
				}
				rf.mu.Unlock()
			}
		}
	}

	rf.mu.Lock()
	if rf.state == Candidate { //如果已经变为follower就失去了变为leader的权利
		rf.state = Leader
		rf.initIndex()
		//rf.leaderID=rf.me
		go rf.tick()
		go rf.norifyNewLeader()
	}
	rf.mu.Unlock()
}

func (rf *Raft) solicit(i int, args RequestVoteArgs, args2 chan RequestVoteReply) {
	var reply RequestVoteReply
	if !rf.peers[i].Call("Raft.RequestVote", &args, &reply) {
		reply.Err = ErrRPCFail //因为不在该函数内处理reply，而是把reply
		reply.Server = i
	}
	args2 <- reply
}

func (rf *Raft) stepDown(i int) {
	rf.currentTerm = i
	rf.state = Follower
	rf.votedFor, rf.leaderID = -1, -1

	rf.persist()

	rf.resetElectionTimer(newRandDuration(ElectTimeout)) //?????????????????????
}

func (rf *Raft) tick() {
	timer := time.NewTimer(AppendEntriesInterval)
	for {
		select {
		case <-rf.shutdown:
			return
		case <-timer.C:
			_, isLeader := rf.GetState()
			if !isLeader {
				return
			}
			go rf.replicate()
			timer.Reset(AppendEntriesInterval)
		}
	}
}

func (rf *Raft) replicate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go rf.sendLogEntry(i)
		}
	}
}

func (rf *Raft) sendLogEntry(i int) {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}

	if rf.nextIndex[i] <= rf.lastIncludeIndex {
		go rf.sendSnapshot(i)
		rf.mu.Unlock()
		return
	}

	args := AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
		//PreLogIndex:  rf.logIndex-1,  !!!!!!!!!!!!!!!!!!!!!!!!!!!
		PreLogIndex:  rf.nextIndex[i] - 1,
		PreLogTerm:   rf.log[rf.nextIndex[i]-1].LogTerm,
		Entries:      nil,
		LeaderCommit: rf.commitIndex,
	}
	if rf.nextIndex[i] < rf.logIndex { //
		args.Entries = rf.getRangeEntries(rf.nextIndex[i], rf.logIndex)
	}
	rf.mu.Unlock()

	var reply AppendEntriesReply
	if rf.peers[i].Call("Raft.AppendEntries", &args, &reply) { //超时就是失败
		rf.mu.Lock()
		if !reply.Success { //失败
			if reply.Term > rf.currentTerm {
				rf.stepDown(reply.Term)
			} else {
				//code 2B
				//nextIndex := rf.nextIndex[i] - 1
				//if nextIndex >=0 {
				//	rf.nextIndex[i] = nextIndex
				//} else {
				//	rf.nextIndex[i] = 0
				//}
				//code 2C
				rf.nextIndex[i] = Max(1, Min(reply.ConflictIndex, rf.logIndex)) // ?????
				if rf.nextIndex[i] <= rf.lastIncludeIndex {                     // follower中的数据丢失了
					go rf.sendSnapshot(i)
				}
				//rf.nextIndex[i] -= 1 //不会小于0，因为所有的server的log的第一项都是相同的空项  未必，因为有可能会因为Term的原因继续减小为负数
			}
		} else { //发送成功，
			prevLogIndex, logEntriesLen := args.PreLogIndex, len(args.Entries)
			rf.nextIndex[i] = prevLogIndex + logEntriesLen + 1
			rf.matchIndex[i] = prevLogIndex + logEntriesLen
			toCommitIndex := prevLogIndex + logEntriesLen
			if rf.canCommit(toCommitIndex) { //根据matchIndex来判断是否可以commit
				rf.commitIndex = toCommitIndex
				rf.persist() //???????
				rf.notifyApplyCh <- struct{}{}
			}
		}

		rf.mu.Unlock()
	}
}

func (rf *Raft) initIndex() {
	peersNum := len(rf.peers)
	rf.nextIndex, rf.matchIndex = make([]int, peersNum), make([]int, peersNum)
	for i := 0; i < peersNum; i++ {
		rf.nextIndex[i] = rf.logIndex
		rf.matchIndex[i] = 0
	}
}

func (rf *Raft) apply() { //apply
	for {
		select {
		case <-rf.shutdown:
			return
		case <-rf.notifyApplyCh:
			rf.mu.Lock()
			var commandValid bool
			var entries []LogEntry
			if rf.lastApplied < rf.lastIncludeIndex { //这意思是说
				commandValid = false
				rf.lastApplied = rf.lastIncludeIndex //？？？？？
				entries = []LogEntry{{
					LogIndex: rf.lastIncludeIndex,
					LogTerm:  rf.log[0].LogTerm,
					Commond:  "InstallSnapshot", //？？？？？？？？？？
				}}
			} else if rf.lastApplied < rf.logIndex && rf.lastApplied < rf.commitIndex { //需要这个判断吗？好像不需要啊！
				commandValid = true
				entries = rf.getRangeEntries(rf.lastApplied+1, rf.commitIndex+1)
				rf.lastApplied = rf.commitIndex
			}
			rf.persist() //??????????????????需要吗？
			rf.mu.Unlock()
			for _, entry := range entries {
				rf.applyCh <- ApplyMsg{ //
					CommandValid: commandValid,
					Command:      entry.Commond,
					CommandIndex: entry.LogIndex}
			}
		}
	}
}

//???????????????????
func (rf *Raft) getRangeEntries(fromInclusive int, toExclusive int) []LogEntry {
	fromInclusive = rf.getOffsetIndex(fromInclusive)
	toExclusive = rf.getOffsetIndex(toExclusive)
	//entries := [] LogEntry{}
	//for i := fromInclusive; i < toExclusive; i++ {
	//	entries = append(entries, rf.log[i])
	//}
	//return entries
	return append([]LogEntry{}, rf.log[fromInclusive:toExclusive]...)
}

func (rf *Raft) norifyNewLeader() { //告诉服务器新的leader是谁？
	rf.applyCh <- ApplyMsg{
		CommandValid: false,
		Command:      "NewLeader",
		CommandIndex: -1,
	}
}

func (rf *Raft) canCommit(i int) bool {
	if i < rf.logIndex && rf.commitIndex < i && rf.log[i].LogTerm == rf.currentTerm {
		majority, count := len(rf.peers)/2+1, 0
		for j := 0; j < len(rf.peers); j++ {
			if rf.matchIndex[j] >= i {
				count += 1
			}
		}
		return count >= majority
	} else {
		return false
	}
}

func (rf *Raft) sendSnapshot(i int) {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	args := InstallSnapshotArgs{
		Term:             rf.currentTerm,
		LeaderId:         rf.me,
		LastIncludeIndex: rf.lastIncludeIndex,
		LastIncludeTerm:  rf.getEntry(rf.lastIncludeIndex).LogTerm,
		Data:             rf.persister.ReadSnapshot(),
	}
	rf.mu.Unlock()
	var reply InstallSnapshotReply
	if rf.peers[i].Call("Raft.InstallSnapshot", &args, &reply) {
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.stepDown(reply.Term)
		} else {
			rf.nextIndex[i] = Max(rf.nextIndex[i], rf.lastIncludeIndex+1)
			rf.matchIndex[i] = Max(rf.matchIndex[i], rf.lastIncludeIndex)
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) getEntry(i int) LogEntry {
	offsetIndex := rf.getOffsetIndex(i)
	return rf.log[offsetIndex]
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
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.leaderID = -1
	rf.shutdown = make(chan struct{}) //?????????
	rf.timer = time.NewTimer(newRandDuration(ElectTimeout))

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.logIndex = 1                             //???
	rf.log = []LogEntry{{0, 0, nil}}            //log entry at index 0 is unused
	rf.applyCh = applyCh                        // 每个raft分别对应于一个applyCh
	rf.notifyApplyCh = make(chan struct{}, 100) //?????

	rf.lastIncludeIndex = 0 //???????????????
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.apply() //apply goroutine
	go func() {
		for { //elect timeout goroutine
			select {
			case <-rf.timer.C:
				rf.campaign()
			case <-rf.shutdown:
				return
			}
		}
	}()
	return rf
}
