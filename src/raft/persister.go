package raft

import "sync"

type Persister struct {
	mu        sync.Mutex
	raftState []byte
	snapshot  []byte
}

func MakePersister() *Persister {
	return &Persister{}
}

func (ps *Persister) Copy() *Persister {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	np := MakePersister()
	np.raftState = ps.raftState
	np.snapshot = ps.snapshot

	return np
}

func (ps *Persister) SaveRaftSate(data []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	ps.raftState = data
}

func (ps *Persister) ReadRaftState() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	return ps.raftState
}

func (ps *Persister) RaftStateSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	return len(ps.raftState)
}

func (ps *Persister) SaveSnapshot(snapshot []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	ps.snapshot = snapshot
}

func (ps *Persister) ReadSnapshot() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	return ps.snapshot
}
