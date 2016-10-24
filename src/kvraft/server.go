package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type OpType int

const (
	GET = OpType(iota)
	PUT
	APPEND
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type  OpType
	Key   string
	Value string
	Cid   int64
	Rid   int64
}

const APPEND_TIMEOUT = 200

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db          map[string]string
	applyResult map[int]chan Op
	dupTable    map[int64]int64
}

func (kv *RaftKV) checkDup(cid, rid int64) bool {
	r, ok := kv.dupTable[cid]
	if ok {
		return r >= rid
	}
	return false
}

func (kv *RaftKV) AppendLogEntry(entry Op) bool {
	index, _, isLeader := kv.rf.Start(entry)
	if !isLeader {
		return false
	}

	kv.mu.Lock()
	ch, ok := kv.applyResult[index]
	if !ok {
		ch = make(chan Op, 1)
		kv.applyResult[index] = ch
	}
	kv.mu.Unlock()

	select {
	case op := <-ch:
		return op == entry
	case <-time.After(APPEND_TIMEOUT * time.Millisecond):
		return false
	}
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	entry := Op{
		Type: GET,
		Key:  args.Key,
		Cid:  args.Cid,
		Rid:  args.Rid,
	}
	ok := kv.AppendLogEntry(entry)
	if !ok {
		reply.WrongLeader = true
	} else {
		reply.WrongLeader = false
		reply.Err = OK
		kv.mu.Lock()
		reply.Value = kv.db[args.Key]
		kv.mu.Unlock()
	}
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	entry := Op{
		Key:   args.Key,
		Value: args.Value,
		Cid:   args.Cid,
		Rid:   args.Rid,
	}
	if args.Op == "Put" {
		entry.Type = PUT
	} else if args.Op == "Append" {
		entry.Type = APPEND
	}

	ok := kv.AppendLogEntry(entry)
	if !ok {
		reply.WrongLeader = true
	} else {
		reply.WrongLeader = false
		reply.Err = OK
	}
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *RaftKV) Apply() {
	for {
		applyMsg := <-kv.applyCh
		op := applyMsg.Command.(Op)
		kv.mu.Lock()

		if kv.checkDup(op.Cid, op.Rid) == false {
			switch op.Type {
			case PUT:
				kv.db[op.Key] = op.Value
			case APPEND:
				kv.db[op.Key] += op.Value
			}
		}
		kv.dupTable[op.Cid] = op.Rid

		ch, ok := kv.applyResult[applyMsg.Index]
		if !ok {
			ch = make(chan Op, 1)
			kv.applyResult[applyMsg.Index] = ch
		}

		kv.mu.Unlock()
		ch <- op
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// Your initialization code here.
	kv.db = make(map[string]string)
	kv.applyResult = make(map[int]chan Op)
	kv.dupTable = make(map[int64]int64)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.Apply()

	return kv
}
