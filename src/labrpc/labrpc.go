package labrpc

import (
	"bytes"
	"encoding/gob"
	"log"
	"math/rand"
	"reflect"
	"strings"
	"sync"
	"time"
)

/*
*类似于Go的RPC系统，但是带有模拟网络
*1.这个模拟网络会延迟请求和回复
*2.这个模拟网络会丢失请求和回复
*3.这个模拟网络会重新排序请求和回复
 */

type reqMsg struct {
	endname  interface{}
	svcMeth  string
	argsType reflect.Type
	args     []byte
	replyCh  chan replyMsg
}

type replyMsg struct {
	ok    bool
	reply []byte
}

type ClientEnd struct {
	endname interface{}
	ch      chan reqMsg //Network.endCh的副本
}

//发送一个RPC，等待回复
//使用反射查找参数类型，使用“gob”序列化参数
//ClientEnd.ch（chan reqMsg）是用于发送请求的通道 e.ch <- req
//需要一个通道从接收回复(<- req.replyCh)
func (e *ClientEnd) Call(svcMeth string, args interface{}, reply interface{}) bool {
	req := reqMsg{}
	req.endname = e.endname
	req.svcMeth = svcMeth
	req.argsType = reflect.TypeOf(args)
	req.replyCh = make(chan replyMsg)

	qb := new(bytes.Buffer)
	qe := gob.NewEncoder(qb)
	qe.Encode(args)
	req.args = qb.Bytes()

	e.ch <- req

	rep := <-req.replyCh
	if rep.ok {
		rb := bytes.NewBuffer(rep.reply)
		rd := gob.NewDecoder(rb)
		if err := rd.Decode(reply); err != nil {
			log.Fatalf("ClientEnd.Call(): decode reply: %v\n", err)
		}
		return true
	} else {
		return false
	}
}

type Network struct {
	mu             sync.Mutex
	reliable       bool
	longDelays     bool //在无法连接时暂停一下
	longReordering bool
	ends           map[interface{}]*ClientEnd
	enabled        map[interface{}]bool
	servers        map[interface{}]*Server
	connections    map[interface{}]interface{}
	endCh          chan reqMsg
}

//新建一个Network
func MakeNetwork() *Network {
	rn := &Network{}
	rn.reliable = true
	rn.ends = map[interface{}]*ClientEnd{}
	rn.enabled = map[interface{}]bool{}
	rn.servers = map[interface{}]*Server{}
	rn.connections = map[interface{}](interface{}){}
	rn.endCh = make(chan reqMsg)

	go func() {
		for xreq := range rn.endCh {
			go rn.ProcessReq(xreq)
		}
	}()

	return rn
}

func (rn *Network) Reliable(yes bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	rn.reliable = yes
}

func (rn *Network) LongReordering(yes bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	rn.longReordering = yes
}

func (rn *Network) LongDelays(yes bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	rn.longDelays = yes
}

//读取端信息
func (rn *Network) ReadEndnameInfo(endname interface{}) (enabled bool,
	servername interface{}, server *Server, reliable bool, longreordering bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	enabled = rn.enabled[endname]
	servername = rn.connections[endname]
	if servername != nil {
		server = rn.servers[servername]
	}
	reliable = rn.reliable
	longreordering = rn.longReordering
	return
}

//判断服务器是否宕机
func (rn *Network) IsServerDead(endname interface{}, servername interface{}, server *Server) bool {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if rn.enabled[endname] == false || rn.servers[servername] != server {
		return true
	}
	return false
}

//处理request
func (rn *Network) ProcessReq(req reqMsg) {
	enabled, servername, server, reliable, longreordering := rn.ReadEndnameInfo(req.endname)

	if enabled && servername != nil && server != nil {
		//小的延迟
		if reliable == false {
			ms := (rand.Int() % 27)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}

		//取消request
		if reliable == false && (rand.Int()%1000) < 100 {
			req.replyCh <- replyMsg{false, nil}
			return
		}

		//并发执行
		ech := make(chan replyMsg)
		go func() {
			r := server.dispatch(req)
			ech <- r
		}()

		//不断等待消息
		var reply replyMsg
		replyOK := false
		serverDead := false
		for replyOK == false && serverDead == false {
			select {
			case reply = <-ech:
				replyOK = true
			case <-time.After(100 * time.Millisecond):
				serverDead = rn.IsServerDead(req.endname, servername, server)
			}
		}

		serverDead = rn.IsServerDead(req.endname, servername, server)

		if replyOK == false || serverDead == true {
			req.replyCh <- replyMsg{false, nil}
		} else if reliable == false && (rand.Int()%1000) < 100 {
			req.replyCh <- replyMsg{false, nil}
		} else if longreordering == true && rand.Intn(900) < 600 {
			ms := 200 + rand.Intn(1+rand.Intn(2000))
			time.Sleep(time.Duration(ms) * time.Millisecond)
			req.replyCh <- reply
		} else {
			req.replyCh <- reply
		}
	} else {
		//如果没有回复，直到响应时间用完
		ms := 0
		if rn.longDelays {
			ms = (rand.Int() % 7000)
		} else {
			ms = (rand.Int() % 100)
		}
		time.Sleep(time.Duration(ms) * time.Millisecond)
		req.replyCh <- replyMsg{false, nil}
	}
}

//创造一个端节点，增加一个线程
func (rn *Network) MakeEnd(endname interface{}) *ClientEnd {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if _, ok := rn.ends[endname]; ok {
		log.Fatalf("MakeEnd: %v already exists\n", endname)
	}

	e := &ClientEnd{}
	e.endname = endname
	e.ch = rn.endCh
	rn.ends[endname] = e
	rn.enabled[endname] = false
	rn.connections[endname] = nil

	return e
}

//增加一台服务器
func (rn *Network) AddServer(servername interface{}, rs *Server) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	rn.servers[servername] = rs
}

//删除一台服务器
func (rn *Network) DeleteServer(servername interface{}) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	rn.servers[servername] = nil
}

//将客户端与服务器相连
//在一个生命周期中只能相连一次
func (rn *Network) Connect(endname interface{}, servername interface{}) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	rn.connections[endname] = servername
}

//使能一个客户端
func (rn *Network) Enable(endname interface{}, enabled bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	rn.enabled[endname] = enabled
}

//统计服务器中的RPC数量
func (rn *Network) GetCount(servername interface{}) int {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	svr := rn.servers[servername]
	return svr.GetCount()
}

/*
*Server
 */

//服务器是服务的集合，其服务共享相同的rpc dispatcher
type Server struct {
	mu       sync.Mutex
	services map[string]*Service
	count    int
}

//创建一个服务器
func MakeServer() *Server {
	rs := &Server{}
	rs.services = map[string]*Service{}
	return rs
}

//增加服务，可以被多个goroutine调用（因为有解锁机制）
func (rs *Server) AddService(svc *Service) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	rs.services[svc.name] = svc
}

//分发请求
func (rs *Server) dispatch(req reqMsg) replyMsg {
	rs.mu.Lock()

	rs.count += 1

	//分割svcMeth，分为service和method
	dot := strings.LastIndex(req.svcMeth, ".")
	serviceName := req.svcMeth[:dot]
	methodName := req.svcMeth[dot+1:]

	service, ok := rs.services[serviceName]

	rs.mu.Unlock()

	if ok {
		return service.dispatch(methodName, req)
	} else {
		choices := []string{}
		for k, _ := range rs.services {
			choices = append(choices, k)
		}
		log.Fatalf("labrpc.Server.dispatch(): unknown service %v in %v.%v; expecting one of %v\n",
			serviceName, serviceName, methodName, choices)
		return replyMsg{false, nil}
	}
}

func (rs *Server) GetCount() int {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.count
}

//一个服务器可能包含多个服务
type Service struct {
	name    string
	rcvr    reflect.Value
	typ     reflect.Type
	methods map[string]reflect.Method
}

//创建一个新的服务
func MakeService(rcvr interface{}) *Service {
	svc := &Service{}
	svc.typ = reflect.TypeOf(rcvr)
	svc.rcvr = reflect.ValueOf(rcvr)
	svc.name = reflect.Indirect(svc.rcvr).Type().Name()
	svc.methods = map[string]reflect.Method{}

	for m := 0; m < svc.typ.NumMethod(); m++ {
		method := svc.typ.Method(m)
		mtype := method.Type
		mname := method.Name

		if method.PkgPath != "" || // capitalized?
			mtype.NumIn() != 3 ||
			//mtype.In(1).Kind() != reflect.Ptr ||
			mtype.In(2).Kind() != reflect.Ptr ||
			mtype.NumOut() != 0 {
			// the method is not suitable for a handler
			//fmt.Printf("bad method: %v\n", mname)
		} else {
			// the method looks like a handler
			svc.methods[mname] = method
		}
	}
	return svc
}

func (svc *Service) dispatch(methname string, req reqMsg) replyMsg {
	if method, ok := svc.methods[methname]; ok {
		args := reflect.New(req.argsType)

		//解码一个参数
		ab := bytes.NewBuffer(req.args)
		ad := gob.NewDecoder(ab)
		ad.Decode(args.Interface())

		replyType := method.Type.In(2)
		replyType = replyType.Elem()
		replyv := reflect.New(replyType)

		function := method.Func
		function.Call([]reflect.Value{svc.rcvr, args.Elem(), replyv})

		rb := new(bytes.Buffer)
		re := gob.NewEncoder(rb)
		re.Encode(replyv)

		return replyMsg{true, rb.Bytes()}
	} else {
		choices := []string{}
		for k, _ := range svc.methods {
			choices = append(choices, k)
		}
		log.Fatalf("labrpc.Service.dispatch(): unknown method %v in %v; expecting one of %v\n",
			methname, req.svcMeth, choices)
		return replyMsg{false, nil}
	}
}
