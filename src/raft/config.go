package raft

import (
	"encoding/base64"
	"sync"
	"testing"
	"runtime"
	"fmt"
	"log"
	"sync/atomic"
)

func randstring(n int) string {
	b := make([]byte,2*n)
	crand.Read(b)
	s := base64.URLEncoding.EncodeToString(b)
	return s[0:n]
}

type config struct {
	mu sync.Mutex
	t *testing.T
	net *labrpc.Network
	n int
	done int32
	rafts []*Raft
	applyErr []string
	connected []bool
	saved []*Persister
	endnames [][]string
	logs []map[int]int
}

func make_config(t *testing.T,n int,unreliable bool) *config {
	runtime.GOMAXPROCS(4)
	cfg := &config{}
	cfg.t = t
	cfg.net = labrpc.MakeNetwork()
	cfg.n = n
	cfg.applyErr = make([]string,cfg.n)
	cfg.rafts = make([]*Raft,cfg.n)
	cfg.connected = make([]bool,cfg.n)
	cfg.saved = make([]*Persister,cfg.n)
	cfg.endnames = make([][]string,cfg.n)
	cfg.logs = make([]map[int]int,cfg.n)

	cfg.setunreliable(unreliable)

	cfg.net.LongDelays(true)

	for i:= 0;i<cfg.n;i++ {
		cfg.logs[i] = map[int]int{}
		cfg.start1(i)
	}
	for i:= 0;i<cfg.n;i++ {
		cfg.connect(i)
	}

	return cfg
}

func (cfg *config) crash1(i int) {
	cfg.disconnect(i)
	cfg.net.DeleteServer(i)

	cfg.mu.Lock()
	defer cfg.mu.Unlock()

	if cfg.saved[i] != nil {
		cfg.saved[i] = cfg.saved[i].Copy()
	}

	rf := cfg.rafts[i]
	if rf != nil {
		cfg.mu.Unlock()
		rf.Kill()
		cfg.mu.Lock()
		cfg.rafts[i] = nil
	}

	if cfg.saved[i] != nil {
		raftLog := cfg.saved[i].ReadRaftState()
		cfg.saved[i] = &Persister{}
		cfg.saved[i].SaveRaftSate(raftLog)
	}
}

func (cfg *config) start1(i int) {
	cfg.crash1(i)

	cfg.endnames[i] = make([]string,cfg.n)
	for j:= 0;j<cfg.n;j++ {
		cfg.endnames[i][j] = randstring(20)
	}

	ends := make([]*labrpc.ClientEnd,cfg.n)
	for j:= 0;j<cfg.n;j++ {
		ends[j] = cfg.net.MakeEnd(cfg.endnames[i][j])
		cfg.net.Connect(cfg.endnames[i][j],j)
	}

	cfg.mu.Lock()

	if cfg.saved[i] !+ nil {
		cfg.saved[i] = cfg.saved[i].Copy()
	} else {
		cfg.saved[i] = MakePersister()
	}

	cfg.mu.Unlock()

	applyCh := make(chan ApplyMsg)
	go func() {
		for m:= range  applyCh {
			err_msg := ""
			if m.UseSnapshot {

			} else if v,ok := (m.Command).(int);ok {
				cfg.mu.Lock()
				for j:= 0;j <len(cfg.logs);j++ {
					if old,oldok := cfg.logs[j][m.Index];oldok && old != v {
						err_msg = fmt.Sprintf("commit index=%v server=%v %v != server=%v %v",
							m.Index, i, m.Command, j, old)
					}
				}
				_,prevok := cfg.logs[i][m.Index-1]
				cfg.logs[i][m.Index] = v
				cfg.mu.Unlock()

				if m.Index > 1 && prevok == false {
					err_msg = fmt.Sprintf("server %v apply out of order %v", i, m.Index)
				}
			} else {
				err_msg = fmt.Sprintf("committed command %v is not an int", m.Command)
			}

			if err_msg != "" {
				log.Fatalf("apply error: %v\n", err_msg)
				cfg.applyErr[i] = err_msg
			}
		}
	}()

	rf := Make(ends,i,cfg.saved[i],applyCh)

	cfg.mu.Lock()
	cfg.rafts[i] = rf
	cfg.mu.Unlock()

	svc := labrpc.MakeService(rf)
	srv := labrpc.MakeServer()
	srv.AddService(svc)
	cfg.net.AddServer(i,srv)
}

func (cfg *config) cleanup() {
	for i:= 0;i<len(cfg.rafts);i++ {
		if cfg.rafts[i] != nil {
			cfg.rafts[i].Kill()
		}
	}
	atomic.StoreInt32(&cfg.done,1)
}

func (cfg *config) connect(i int) {
	cfg.connected[i] = true

	for j:= 0;j<cfg.n;j++ {
		if cfg.connected[j] {
			endname := cfg.endnames[i][j]
			cfg.net.Enable(endname,true)
		}
	}

	for j:= 0;j<cfg.n;j++ {
		if cfg.connected[j] {
			endname := cfg.endnames[j][i]
			cfg.net.Enable(endname,true)
		}
	}
}

func (cfg *config) disconnect(i int) {
	cfg.connected[i] = false

	for j:= 0;j<cfg.n;j++ {
		if cfg.endnames[i] != nil {
			endname:=cfg.endnames[i][j]
			cfg.net.Enable(endname,false)
		}
	}

	for j:= 0;j<cfg.n;j++ {
		if cfg.endnames[i] != nil {
			endname:=cfg.endnames[j][i]
			cfg.net.Enable(endname,false)
		}
	}
}

func (cfg *config) rpcCount(server int) int {
	return cfg.net.GetCount(server)
}

func (cfg *config) setunreliable(unrel bool) {
	cfg.net.Reliable(!unrel)
}

func (cfg *config) setlongreordering(longrel bool) {
	cfg.net.LongReordering(longrel)
}

