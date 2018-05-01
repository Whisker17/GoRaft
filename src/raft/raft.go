package raft

import (
	"bytes"
	"encoding/gob"
	"math/rand"
	"sync"
	"time"
)

type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool
	Snapshot    []byte
}

const (
	STOPPED      = "stopped"
	INITIALIZED  = "initialized"
	FOLLOWER     = "follower"
	CANDIDATE    = "candidate"
	LEADER       = "leader"
	SNAPSHOTTING = "snapshotting"
)

const (
	HeartbeatCycle  = time.Millisecond * 50
	ElectionMinTime = 150
	ElectionMaxTime = 300
)

type LogEntry struct {
	Command interface{}
	Term    int
}

type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int

	currentTerm int
	votedFor    int
	logs        []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	granted_voyes_count int

	state   string
	applyCh chan ApplyMsg

	timer *time.Timer
}

func (rf *Raft) persist() {
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	enc.Encode(rf.currentTerm)
	enc.Encode(rf.votedFor)
	enc.Encode(rf.logs)
	rf.persister.SaveRaftSate(buf.Bytes())
}

func (rf *Raft) readPersist(data []byte) {
	if data != nil {
		buf := bytes.NewBuffer(data)
		dec := gob.NewDecoder(buf)
		dec.Decode(&rf.currentTerm)
		dec.Decode(&rf.votedFor)
		dec.Decode(&rf.logs)
	}
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	may_grant_vote := true

	if len(rf.logs) > 0 {
		if rf.logs[len(rf.logs)-1].Term > args.LastLogIndex ||
			(rf.logs[len(rf.logs)-1].Term == args.LastLogIndex &&
				len(rf.logs)-1 > args.LastLogIndex) {
			may_grant_vote = false
		}
	}

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	} else if args.Term == rf.currentTerm {
		if rf.votedFor == -1 && may_grant_vote {
			rf.votedFor = args.CandidateId
			rf.persist()
		}
		reply.Term = rf.currentTerm
		reply.VoteGranted = (rf.votedFor == args.CandidateId)

		return
	} else {
		rf.state = FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = -1

		if may_grant_vote {
			rf.votedFor = args.CandidateId
			rf.persist()
		}
		rf.resetTimer()

		reply.Term = args.Term
		reply.VoteGranted = (rf.votedFor == args.CandidateId)

		return
	}
}

func majority(n int) int {
	return n/2 + 1
}

func (rf *Raft) handleVoteResult(reply RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term < rf.currentTerm {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.resetTimer()
		return
	}

	if rf.state == CANDIDATE && reply.VoteGranted {
		rf.granted_voyes_count += 1
		if rf.granted_voyes_count >= majority(len(rf.peers)) {
			rf.state = LEADER
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				rf.nextIndex[i] = len(rf.logs)
				rf.matchIndex[i] = -1
			}
			rf.resetTimer()
		}
		return
	}
}

func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntryArgs struct {
	Term         int
	Leader_id    int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntryReply struct {
	Term        int
	Success     bool
	CommitIndex int
}

func (rf *Raft) AppendEntries(args AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
	} else {
		rf.state = FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = -1
		reply.Term = args.Term

		if args.PrevLogIndex >= 0 &&
			(len(rf.logs)-1 < args.PrevLogIndex || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm) {
			reply.CommitIndex = len(rf.logs) - 1
			if reply.CommitIndex > args.PrevLogIndex {
				reply.CommitIndex = args.PrevLogIndex
			}
			for reply.CommitIndex >= 0 {
				if rf.logs[reply.CommitIndex].Term == args.PrevLogTerm {
					break
				}
				reply.CommitIndex--
			}
			reply.Success = false
		} else if args.Entries != nil {
			rf.logs = rf.logs[:args.PrevLogIndex+1]
			rf.logs = append(rf.logs, args.Entries...)
			if len(rf.logs)-1 >= args.LeaderCommit {
				rf.commitIndex = args.LeaderCommit
				go rf.commitLogs()
			}
			reply.CommitIndex = len(rf.logs) - 1
			reply.Success = true
		}
	}
	rf.persist()
	rf.resetTimer()
}

func (rf *Raft) SendAppendEntryToFollower(server int, args AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) SendAppendEntriesToAllFollower() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		var args AppendEntryArgs
		args.Term = rf.currentTerm
		args.Leader_id = rf.me
		args.PrevLogIndex = rf.nextIndex[i] - 1

		if args.PrevLogIndex >= 0 {
			args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
		}
		if rf.nextIndex[i] < len(rf.logs) {
			args.Entries = rf.logs[rf.nextIndex[i]:]
		}
		args.LeaderCommit = rf.commitIndex

		go func(server int, args AppendEntryArgs) {
			var reply AppendEntryReply
			ok := rf.SendAppendEntryToFollower(server, args, &reply)
			if ok {
				rf.handleAppendEntries(server, reply)
			}
		}(i, args)
	}
}

func (rf *Raft) handleAppendEntries(server int, reply AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != LEADER {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.resetTimer()
		return
	}

	if reply.Success {
		rf.nextIndex[server] = reply.CommitIndex + 1
		rf.matchIndex[server] = reply.CommitIndex
		reply_count := 1
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			if rf.matchIndex[i] >= rf.matchIndex[server] {
				reply_count += 1
			}
		}
		if reply_count >= majority(len(rf.peers)) &&
			rf.commitIndex < rf.matchIndex[server] &&
			rf.logs[rf.matchIndex[server]].Term == rf.currentTerm {
			rf.commitIndex = rf.matchIndex[server]
			go rf.commitLogs()
		}
	} else {
		rf.nextIndex[server] = reply.CommitIndex + 1
		rf.SendAppendEntriesToAllFollower()
	}
}

func (rf *Raft) commitLogs() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.commitIndex > len(rf.logs)-1 {
		rf.commitIndex = len(rf.logs) - 1
	}

	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		rf.applyCh <- ApplyMsg{Index: i + 1, Command: rf.logs[i].Command}
	}

	rf.lastApplied = rf.commitIndex
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := -1
	isLeader := false
	nlog := LogEntry{command, rf.currentTerm}

	if rf.state != LEADER {
		return index, term, isLeader
	}

	isLeader = (rf.state == LEADER)
	rf.logs = append(rf.logs, nlog)
	index = len(rf.logs)
	term = rf.currentTerm
	rf.persist()

	return index, term, isLeader
}

func (rf *Raft) Kill() {

}

func (rf *Raft) hanleTimer() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != LEADER {
		rf.state = CANDIDATE
		rf.currentTerm += 1
		rf.votedFor = rf.me
		rf.granted_voyes_count = 1
		rf.persist()

		args := RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: len(rf.logs) - 1,
		}

		if len(rf.logs) > 0 {
			args.LastLogTerm = rf.logs[args.LastLogIndex].Term
		}

		for server := 0; server < len(rf.peers); server++ {
			if server == rf.me {
				continue
			}

			go func(server int, args RequestVoteArgs) {
				var reply RequestVoteReply
				ok := rf.sendRequestVote(server, args, &reply)
				if ok {
					rf.handleVoteResult(reply)
				}
			}(server, args)
		}
	} else {
		rf.SendAppendEntriesToAllFollower()
	}
	rf.resetTimer()
}

func (rf *Raft) resetTimer() {
	if rf.timer == nil {
		rf.timer = time.NewTimer(time.Millisecond * 1000)
		go func() {
			for {
				<-rf.timer.C
				rf.hanleTimer()
			}
		}()
	}
	new_timeout := HeartbeatCycle
	if rf.state != LEADER {
		new_timeout = time.Millisecond * time.Duration(ElectionMinTime+rand.Int63n(ElectionMaxTime-ElectionMinTime))
	}
	rf.timer.Reset(new_timeout)
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = make([]LogEntry, 0)

	rf.commitIndex = -1
	rf.lastApplied = -1

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.state = FOLLOWER
	rf.applyCh = applyCh

	rf.readPersist(persister.ReadRaftState())

	rf.persist()
	rf.resetTimer()

	return rf
}
