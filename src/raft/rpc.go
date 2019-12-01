package raft

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//term、votedFor、logTerm、logIndex、reset time
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Err = OK
	reply.Server = rf.me
	if args.Term == rf.currentTerm && rf.votedFor == args.CandidateId {
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		return
	}
	if args.Term < rf.currentTerm ||
		(args.Term == rf.currentTerm && rf.votedFor != -1) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = args.Term, -1 //
		if rf.state != Follower {
			rf.resetElectionTimer(newRandDuration(ElectTimeout))
			rf.state = Follower
		}
	}

	rf.leaderID = -1
	reply.Term = args.Term
	logIndex := rf.logIndex - 1
	logTerm := rf.log[logIndex].LogTerm
	if logTerm > args.LastLogTerm || //check whether up-to-date
		(logTerm == args.LastLogTerm && logIndex > args.LastLogIndex) {
		reply.VoteGranted = false
		return
	}
	reply.VoteGranted = true
	rf.votedFor = args.CandidateId // !!!!!!!!!!!
	rf.resetElectionTimer(newRandDuration(ElectTimeout))
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//term、leaderId、resetTime、一致性检测（通过一致性检测之后，log、commitId、applyId就和leader一致了）、apply
	rf.mu.Lock()
	defer rf.mu.Unlock()            // 如果比较占用时间的话，即便接收到下一个AppendEntries RPC也不能及时重置timer。
	if args.Term < rf.currentTerm { // first consider whether this rpc is legitimate  [ləˈdʒɪtəmɪt]
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	reply.Term = rf.currentTerm
	rf.leaderID = args.LeaderId
	rf.resetElectionTimer(newRandDuration(ElectTimeout)) // 新leader出现，重置elect time
	if args.Term > rf.currentTerm {                      //term
		rf.currentTerm, rf.votedFor = args.Term, -1
	}
	rf.state = Follower
	//
	if args.PreLogIndex < rf.lastIncludeIndex {
		reply.Success = false
		reply.ConflictIndex = rf.lastIncludeIndex + 1 //?????
		return
	}
	if rf.logIndex <= args.PreLogIndex || rf.getEntry(args.PreLogIndex).LogTerm != args.PreLogTerm {
		conflictIndex := Min(rf.logIndex-1, args.PreLogIndex)
		conflictTerm := rf.getEntry(conflictIndex).LogTerm
		floor := Max(rf.lastIncludeIndex, rf.commitIndex)
		for ; conflictIndex > floor && rf.getEntry(conflictIndex-1).LogTerm == conflictTerm; conflictIndex-- {
		}
		reply.ConflictIndex = conflictIndex
		reply.Success = false
		return
	}
	reply.Success = true

	i := 0
	preLogIndex := args.PreLogIndex
	for ; i < len(args.Entries); i++ {
		if preLogIndex+i+1 >= rf.logIndex {
			break
		}
		if rf.getEntry(preLogIndex+1+i).LogTerm != args.Entries[i].LogTerm {
			rf.logIndex = preLogIndex + 1 + i
			truncationEndIndex := rf.getOffsetIndex(rf.logIndex)
			rf.log = append(rf.log[:truncationEndIndex])
			break //!!!!!!!!!!!
		}
	}
	for ; i < len(args.Entries); i++ {
		rf.log = append(rf.log, args.Entries[i])
		rf.logIndex += 1
	}
	//rf.resetElectionTimer(newRandDuration(ElectTimeout))  // ？？
	if args.LeaderCommit > rf.commitIndex { // leader的commitIndex不可能小于follower的commitIndex！
		rf.commitIndex = args.LeaderCommit
		rf.notifyApplyCh <- struct{}{} //
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Err = OK
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	rf.leaderID = args.LeaderId
	if args.LastIncludeIndex > rf.lastIncludeIndex {
		truncationStartIndex := rf.getOffsetIndex(args.LastIncludeIndex)
		rf.lastIncludeIndex = args.LastIncludeIndex
		oldCommitIndex := rf.commitIndex
		rf.commitIndex = Max(rf.commitIndex, rf.lastIncludeIndex)
		rf.logIndex = Max(rf.logIndex, rf.lastIncludeIndex+1)
		if truncationStartIndex < len(rf.log) {
			rf.log = append(rf.log[truncationStartIndex:])
		} else {
			rf.log = []LogEntry{{args.LastIncludeIndex, args.LastIncludeTerm, nil}}
		}
		rf.persister.SaveStateAndSnapshot(rf.getPersistState(), args.Data)
		if rf.commitIndex > oldCommitIndex {
			rf.notifyApplyCh <- struct{}{}
		}
	}
	rf.resetElectionTimer(newRandDuration(ElectTimeout))
	rf.persist()
}

func Max(a, b int) int {
	if a < b {
		return b
	} else {
		return a
	}
}
func Min(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}
