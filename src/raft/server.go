package raft

import (
	"fmt"
	"io"
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type LockServer struct {
	mu    sync.Mutex
	l     net.Listener
	dead  bool
	dying bool

	am_primary bool
	backup     string

	locks map[string]bool
}

func (ls *LockServer) Lock(args *LockArgs, reply *LockReply) error {
	ls.mu.Lock()
	defer ls.mu.Unlock()

	locked, _ := ls.locks[args.Lockname]

	if locked {
		reply.OK = false
	} else {
		reply.OK = true
		ls.locks[args.Lockname] = true
	}

	return nil
}

func (ls *LockServer) Unlock(args *UnlockArgs, reply *UnlockReply) error {
	return nil
}

func (ls *LockServer) kill() {
	ls.dead = true
	ls.l.Close()
}

type DeafConn struct {
	c io.ReadWriteCloser
}

func (dc DeafConn) Write(p []byte) (n int, err error) {
	return len(p), nil
}

func (dc DeafConn) Close() error {
	return dc.c.Close()
}

func (dc DeafConn) Read(p []byte) (n int, err error) {
	return dc.c.Read(p)
}

func StartServer(primary, backup string, am_primary bool) *LockServer {
	ls := new(LockServer)
	ls.backup = backup
	ls.am_primary = am_primary
	ls.locks = map[string]bool{}

	me := ""
	if am_primary {
		me = primary
	} else {
		me = backup
	}

	rpcs := rpc.NewServer()
	rpcs.Register(ls)

	os.Remove(me)
	l, e := net.Listen("unix", me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	ls.l = l

	go func() {
		for ls.dead == false {
			conn, err := ls.l.Accept()
			if err == nil && ls.dead == false {
				if ls.dying {
					go func() {
						time.Sleep(2 * time.Second)
						conn.Close()
					}()
					ls.l.Close()

					deaf_conn := DeafConn{c: conn}

					rpcs.ServeConn(deaf_conn)

					ls.dead = true
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && ls.dead == false {
				fmt.Printf("LockServer(%v) accept: %v\n", me, err.Error())
				ls.kill()
			}
		}
	}()

	return ls
}
