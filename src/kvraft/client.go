package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
import "sync"

type Clerk struct {
	servers  []*labrpc.ClientEnd
	leaderId int
	// You will have to modify this struct.
	uid int64
	rid int64
	mu  sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.leaderId = -1
	// You'll have to add code here.
	ck.uid = nrand()
	ck.rid = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	ck.mu.Lock()
	args := &GetArgs{
		Key: key,
		Cid: ck.uid,
		Rid: ck.rid,
	}
	ck.rid++
	ck.mu.Unlock()

	for {
		if ck.leaderId != -1 {
			reply := &GetReply{}
			ok := ck.servers[ck.leaderId].Call("RaftKV.Get", args, reply)
			if ok && !reply.WrongLeader {
				return reply.Value
			}
		}

		for i, v := range ck.servers {
			reply := &GetReply{}
			ok := v.Call("RaftKV.Get", args, reply)
			if ok && !reply.WrongLeader {
				ck.leaderId = i
				return reply.Value
			}
		}
	}

	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.mu.Lock()
	args := &PutAppendArgs{
		Op:    op,
		Key:   key,
		Value: value,
		Cid:   ck.uid,
		Rid:   ck.rid,
	}
	ck.rid++
	ck.mu.Unlock()

	for {
		if ck.leaderId != -1 {
			reply := &PutAppendReply{}
			ok := ck.servers[ck.leaderId].Call("RaftKV.PutAppend", args, reply)
			if ok && !reply.WrongLeader {
				return
			}
		}

		for i, v := range ck.servers {
			reply := &PutAppendReply{}
			ok := v.Call("RaftKV.PutAppend", args, reply)
			if ok && !reply.WrongLeader {
				ck.leaderId = i
				return
			}
		}
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
