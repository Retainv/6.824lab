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
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"labrpc"
)

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

var (
	LEADER    = 0
	FOLLOWER  = 1
	CANDIDATE = 2

	HEARTBEAT = -1

	HEARTBEATTIMEOUT = 120 * time.Millisecond

	NO_VOTE = -1
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	logs        []LogEntry // 日志数组

	heartbeatTimer *time.Timer
	electionTimer  *time.Timer

	stopCh chan struct{} //首尾呼应，用来优雅关闭

	// 2A选举参数
	role       int
	inElection bool // 是否正在进行选举
	interrupt  bool // 选举过程中是否收到leader心跳
	votedTerm  int  // 投票的term

	// 2B日志参数
	commitIndex int
	lastApplied int

	// 2B -leader
	nextIndex  []int
	matchIndex []int

	applyCh chan ApplyMsg
}

type LogEntry struct {
	Term    int
	Command interface{}
	Index   int
}

type AppendEntryArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
	LeaderTerm   int // 心跳连接同步term
}
type AppendEntryReply struct {
	Term     int
	Success  bool
	NeedSync bool // 是否需要同步日志
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	isLeader := false
	rf.mu.Lock()
	if rf.role == LEADER {
		isLeader = true
	} else {
		isLeader = false
	}
	term := rf.currentTerm
	rf.mu.Unlock()
	// Your code here (2A).
	PrettyDebug(dTrace, "S%d isLeader:%v", rf.me, isLeader)
	return term, isLeader
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

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
	// Your data here (2A, 2B).
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
	// Your code here (2A, 2B).

	reply.Term = rf.currentTerm
	// 如果我是leader并且他的term比我小，则拒绝
	if rf.role == LEADER || args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}

	lastLog := LogEntry{}
	if len(rf.logs) == 0 {
		lastLog = LogEntry{
			Term:  0,
			Index: 0,
		}
	} else {
		lastLog = rf.logs[len(rf.logs)-1]
	}
	// 如果没有投票或者已投给该candidate，并且日志跟我一样或新，则投票给他

	//PrettyDebug(dVote, "candidate %d term: %d, server %d term: %d\n", args.CandidateId, args.Term, rf.me, rf.currentTerm)
	//PrettyDebug(dVote, "S%d 收到S%d 的RequestVote,term:%d, \n", args.CandidateId, rf.me, rf.currentTerm)
	if args.LastLogTerm >= lastLog.Term && args.LastLogIndex >= lastLog.Index && args.Term >= rf.currentTerm {
		// 新一轮term
		if args.Term > rf.votedTerm {
			rf.votedFor = NO_VOTE
		}
		// 如果该term还未投票或者已给他投票，则返回
		if rf.votedFor == NO_VOTE || args.CandidateId == rf.votedFor {
			//PrettyDebug(dVote, "S%d voted for S%d", rf.me, args.CandidateId)
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.votedTerm = args.Term
			rf.persist()
			PrettyDebug(dVote, "S%d投了S%d一票, votedTerm:%d\n", rf.me, args.CandidateId, rf.votedTerm)
		} else {
			reply.VoteGranted = false
			//PrettyDebug(dVote, "S%d拒绝给S%d投票,v:%d\n", rf.me, args.CandidateId, rf.votedFor)
		}

	} else {
		reply.VoteGranted = false
		//PrettyDebug(dVote, "S%d拒绝给S%d投票,v:%d\n", rf.me, args.CandidateId, rf.votedFor)
	}
}

func (rf *Raft) ReceiveAppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term == HEARTBEAT {
		if args.LeaderTerm >= rf.currentTerm {
			if rf.inElection {
				rf.interrupt = true
			}
			rf.role = FOLLOWER
			rf.currentTerm = args.LeaderTerm
			// 心跳重置选举时间
			PrettyDebug(dTerm, "S%d 收到 S%d 心跳, commitIndex:%d", rf.me, args.LeaderId, rf.commitIndex)

			rf.ResetElectionTimer()
			rf.votedFor = NO_VOTE
			// 心跳同步日志
			if args.LeaderCommit > rf.commitIndex {
				// leaderCommit和我的日志最长部分取最小值
				if args.LeaderCommit > len(rf.logs) {
					rf.commitIndex = len(rf.logs)
				} else {
					rf.commitIndex = args.LeaderCommit
				}
				PrettyDebug(dTerm, "S%d reset commitIndex:%d, leaderCommit:%d", rf.me, rf.commitIndex, args.LeaderCommit)
			}
			// 通过applyCh发送日志
			lastApplied := rf.lastApplied
			PrettyDebug(dTerm, "S%d lastApplied:%d", rf.me, rf.lastApplied)
			if lastApplied < rf.commitIndex {
				rf.ApplyCommand()
			}

			rf.AppendLog(args, reply)
			return
		} else {
			reply.Success = false
			reply.NeedSync = false
			return
		}
	}

	rf.AppendLog(args, reply)
}

func (rf *Raft) ApplyCommand() {
	lastApplied := rf.lastApplied
	for i := lastApplied; i < rf.commitIndex; i++ {

		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[i].Command,
			CommandIndex: i + 1,
		}
		rf.applyCh <- msg
		rf.lastApplied++

		PrettyDebug(dInfo, "S%d send log %v to applyCh", rf.me, msg)
		PrettyDebug(dInfo, "S%d lastApplied: %d, commitIndex: %d", rf.me, rf.lastApplied, rf.commitIndex)
	}
}
func (rf *Raft) AppendLog(args *AppendEntryArgs, reply *AppendEntryReply) {
	PrettyDebug(dLog, "S%d received arg: %v", rf.me, args)
	prevIndex, preLogTerm := 0, 0
	if len(rf.logs) > 0 {
		prevEntry := rf.logs[len(rf.logs)-1]
		prevIndex = len(rf.logs)
		preLogTerm = prevEntry.Term
	}
	// 如果日志缺少，返回
	PrettyDebug(dLog, "S%d argsIndex:%d, prevIndex:%d", rf.me, args.PrevLogIndex, prevIndex)

	if args.PrevLogIndex > prevIndex {
		reply.NeedSync = true
		reply.Success = false
		return
	}
	// 如果索引相同但term不同，返回，等待leader发送之前的日志
	if args.PrevLogIndex == prevIndex && args.PrevLogTerm != preLogTerm {
		reply.NeedSync = true
		reply.Success = false
		return
	}
	if len(args.Entries) != 0 {
		// 添加日志
		entries := append(rf.logs, args.Entries...)
		rf.logs = entries
		PrettyDebug(dInfo, "S%d appended log: %v", rf.me, args.Entries)
	}
	PrettyDebug(dInfo, "S%d log: %v", rf.me, rf.logs)

	reply.Success = true
}

func (rf *Raft) StartElection() {
	rf.mu.Lock()
	if rf.role != FOLLOWER {
		//PrettyDebug(dVote, "S%d role为 %d 无法开启选举", rf.me, rf.role)
		return
	}
	//PrettyDebug(dVote, "S%d 开始选举", rf.me)
	rf.inElection = true
	rf.role = CANDIDATE
	lastLog := LogEntry{}
	if len(rf.logs) == 0 {
		lastLog = LogEntry{
			Term:  0,
			Index: 0,
		}
	} else {
		lastLog = rf.logs[len(rf.logs)-1]
	}
	rf.currentTerm++
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLog.Index,
		LastLogTerm:  lastLog.Term,
	}
	// 自己先投一票
	votedForMe := int32(1)
	rf.votedFor = rf.me
	rf.votedTerm = rf.currentTerm
	winElection := false
	group := sync.WaitGroup{}
	rf.mu.Unlock()
	//PrettyDebug(dVote, "S%d started election, candidate term:%d\n", rf.me, rf.currentTerm)
	//PrettyDebug(dTimer,)("peers:%v", len(rf.peers))
	group.Add(len(rf.peers) - 1)

	done := make(chan bool)
	channels := make([]chan bool, len(rf.peers))
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			timeout := make(chan bool)
			channels[i] = timeout
			defer group.Done()
			for {
				select {
				case <-timeout:
					PrettyDebug(dTrace, "S%d->S%d rpc out", rf.me, i)
					return
				default:
					reply := &RequestVoteReply{}
					rf.sendRequestVote(i, args, reply)
					//PrettyDebug(dTrace, "S%d sent RequestVote to S%d", rf.me, i)
					//PrettyDebug(dTrace, "S%d to S%d，reply：%v", rf.me, i, reply)
					// 如果别人的term比我大，则退出
					if reply.Term > rf.currentTerm {
						rf.mu.Lock()
						rf.currentTerm = reply.Term
						rf.votedFor = NO_VOTE
						rf.role = FOLLOWER
						rf.interrupt = true
						rf.mu.Unlock()
						done <- true
						timeout <- true
						//return
					}
					if reply.VoteGranted {
						atomic.AddInt32(&votedForMe, 1)
						//PrettyDebug(dTrace, "S%d got S%d's vote,votes:%d", rf.me, i, atomic.LoadInt32(&votedForMe))
						// 如果票数超过一半，则不再等待
						if atomic.LoadInt32(&votedForMe) > (int32(len(rf.peers)))/2 {
							winElection = true
							done <- true
							//PrettyDebug(dTrace, "S%d win!", rf.me)
						}
						timeout <- true
						//return
					}
					if reply.Term != 0 {
						timeout <- true
						//return
					}
					time.Sleep(100 * time.Millisecond)

				}
			}

		}(i)
	}

	go func() {
		group.Wait()
		done <- true
	}()
	// wait超时
	go func() {
		time.Sleep(400 * time.Millisecond)
		done <- true
		for i := range channels {
			channels[i] <- true
		}
	}()
	<-done
	//PrettyDebug(dVote, "S%v got vote:%v", rf.me, votedForMe)
	// 如果竞选过程中收到原来leader的消息变回了follower
	if rf.interrupt {
		rf.mu.Lock()
		//PrettyDebug(dVote, "S%d 选举过程中收到leader消息或别人term比我大，退出选举", rf.me)
		//rf.currentTerm--
		rf.votedFor = NO_VOTE
		rf.role = FOLLOWER
		rf.inElection = false
		rf.interrupt = false
		rf.ResetElectionTimer()
		rf.mu.Unlock()
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if winElection {
		PrettyDebug(dVote, "S%d 成为leader,选举成功！", rf.me)
		rf.role = LEADER
		rf.SendHeartBeat()
		rf.electionTimer.Stop()
		rf.heartbeatTimer.Reset(HEARTBEATTIMEOUT)
	} else {
		//rf.currentTerm--
		PrettyDebug(dVote, "S%d 选举失败！", rf.me)
		rf.role = FOLLOWER
		rf.ResetElectionTimer()
	}
	rf.votedFor = NO_VOTE
	rf.inElection = false

}

func (rf *Raft) SendHeartBeat() {

	if rf.role != LEADER {
		return
	}
	PrettyDebug(dLeader, "S%d is sending heartbeat, role: %d, term: %d", rf.me, rf.role, rf.currentTerm)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.CallHeartBeat(i)
	}
	PrettyDebug(dLeader, "S%d sent heartbeat success", rf.me)
}

func (rf *Raft) CallHeartBeat(i int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	prevIndex, preLogTerm := 0, 0
	if len(rf.logs) > 0 {
		prevEntry := rf.logs[len(rf.logs)-1]
		prevIndex = len(rf.logs)
		preLogTerm = prevEntry.Term
	}

	// 如果和我日志一样，则不需要同步
	args := &AppendEntryArgs{
		Term:         HEARTBEAT,
		LeaderId:     rf.me,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  preLogTerm,
		Entries:      nil,
		LeaderTerm:   rf.currentTerm,
		LeaderCommit: rf.commitIndex,
	}
	// 否则发送nextIndex到后面所有的日志
	PrettyDebug(dLog, "S%d nextIndex:%v", rf.me, rf.nextIndex)
	if rf.nextIndex[i] < len(rf.logs) {
		prevIndex, preLogTerm := 0, 0
		if rf.nextIndex[i] > 1 {
			prevEntry := rf.logs[rf.nextIndex[i]-2]
			prevIndex = prevEntry.Index
			preLogTerm = prevEntry.Term
		}
		PrettyDebug(dLog, "S%d->S%d nextIndex:%d,", rf.me, i, rf.nextIndex[i])
		args = &AppendEntryArgs{
			Term:         HEARTBEAT,
			LeaderId:     rf.me,
			PrevLogIndex: prevIndex,
			PrevLogTerm:  preLogTerm,
			Entries:      rf.logs[rf.nextIndex[i]:],
			LeaderTerm:   rf.currentTerm,
			LeaderCommit: rf.commitIndex,
		}
	}

	reply := &AppendEntryReply{}
	rf.peers[i].Call("Raft.ReceiveAppendEntries", args, reply)
	PrettyDebug(dLog, "S%d->S%d reply:%v", rf.me, i, reply)
	PrettyDebug(dLog, "S%d->S%d 补发：%v", rf.me, i, args.Entries)

	if reply.Success == false {
		if reply.NeedSync {
			rf.nextIndex[i]--
			PrettyDebug(dLog, "S%d->S%d nextIndex--:%d", rf.me, i, rf.nextIndex[i])
		}
	} else {
		rf.nextIndex[i] = len(rf.logs) + 1
		rf.matchIndex[i] = len(rf.logs)
	}
	// 如果别人的term比我大，则卸任leader
	//if reply.Term > rf.currentTerm {
	//	rf.role = FOLLOWER
	//}
}

func (rf *Raft) SyncClientRequestLog() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	PrettyDebug(dInfo, "S%d log:%v", rf.me, rf.logs)
	// 如果还没有日志
	prevIndex, preLogTerm := 0, 0
	if len(rf.logs) > 1 {
		// -1因为leader已经将最新的日志加进去了
		prevEntry := rf.logs[len(rf.logs)-2]
		prevIndex = len(rf.logs) - 1
		preLogTerm = prevEntry.Term
	}
	// client新到的一条指令
	args := &AppendEntryArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  preLogTerm,
		Entries:      rf.logs[len(rf.logs)-1:],
		LeaderTerm:   rf.currentTerm,
	}
	successCount := int32(1)
	commitChan := make(chan bool)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			reply := &AppendEntryReply{}

			rf.peers[i].Call("Raft.ReceiveAppendEntries", args, reply)
			PrettyDebug(dInfo, "S%d->S%d reply:%v", rf.me, i, reply)
			if reply.Success {
				rf.nextIndex[i] = len(rf.logs) + 1
				rf.matchIndex[i] = len(rf.logs)
				atomic.AddInt32(&successCount, 1)
				if atomic.LoadInt32(&successCount) > (int32(len(rf.peers)))/2 {
					commitChan <- true
				}
			}
		}(i)
	}
	<-commitChan
	rf.commitIndex = len(rf.logs)
	args.LeaderCommit = rf.commitIndex
	args.Entries = nil
	// todo: 通知server commit
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			reply := &AppendEntryReply{}
			rf.peers[i].Call("Raft.ReceiveAppendEntries", args, reply)
		}(i)
	}

	// leader apply
	rf.ApplyCommand()
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
	PrettyDebug(dLog, "S%d 收到client：%v", rf.me, command)
	if rf.killed() {
		return index, term, false
	}

	if rf.role != LEADER {
		return index, term, false
	}
	PrettyDebug(dLog, "S%d 收到指令加锁", rf.me)
	rf.mu.Lock()
	// 初始化日志
	entry := LogEntry{
		Command: command,
		Term:    rf.currentTerm,
		Index:   len(rf.logs) + 1,
	}
	entries := append(rf.logs, entry)
	rf.logs = entries
	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.logs) + 1
	}
	PrettyDebug(dLog, "S%d nextIndex:%v", rf.me, rf.nextIndex)
	rf.mu.Unlock()

	go rf.SyncClientRequestLog()
	index = len(entries)
	term = rf.currentTerm
	PrettyDebug(dLog, "S%d 同步完成退出 %d,%d,%v", rf.me, index, term, true)
	return index, term, true
	// Your code here (2B).

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
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		select {
		case <-rf.stopCh:
			atomic.StoreInt32(&rf.dead, 1)
			return
		case <-rf.heartbeatTimer.C:
			// leader定期发送心跳
			rf.mu.Lock()
			PrettyDebug(dTrace, "S%d role:%d", rf.me, rf.role)
			if rf.role == LEADER {
				rf.SendHeartBeat()
				rf.heartbeatTimer.Reset(HEARTBEATTIMEOUT)
			}
			rf.mu.Unlock()
		case <-rf.electionTimer.C:
			// leader超时选举
			PrettyDebug(dTrace, "S%d 超时发起选举", rf.me)
			rf.StartElection()
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
	rf.heartbeatTimer = time.NewTimer(HEARTBEATTIMEOUT)
	electionTime := (time.Duration)(400+rand.Int63()%400) * time.Millisecond
	rf.electionTimer = time.NewTimer(electionTime)
	rf.currentTerm = 0
	rf.inElection = false
	rf.role = FOLLOWER
	rf.votedFor = NO_VOTE

	// logs-2b
	rf.logs = make([]LogEntry, 0)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.applyCh = applyCh
	PrettyDebug(dInfo, "[Make] 创建raft peer：heartbeatTimer:%v", HEARTBEATTIMEOUT)
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
func (rf *Raft) ResetElectionTimer() {
	i := 300 + (rand.Int63() % 500)
	electionTime := (time.Duration)(i) * time.Millisecond
	PrettyDebug(dTimer, "S%d reset：%d ms", rf.me, i)
	rf.electionTimer.Reset(electionTime)
}
