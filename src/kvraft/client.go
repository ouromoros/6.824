package raftkv

import (
	"crypto/rand"
	"labrpc"
	"math/big"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	prevLeader int
	prevIndex int
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
	// You'll have to add code here.
	ck.prevLeader = 0
	ck.prevIndex = 1
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	var args GetArgs

	args.Key = key
	args.PrevIndex = ck.prevIndex
	args.Identifier = nrand()

	numServer := len(ck.servers)
	for i := 0; ; i = (i + 1) % numServer {
		var reply GetReply
		server := (i + ck.prevLeader) % numServer
		ok := ck.servers[server].Call("KVServer.Get", &args, &reply)
		if ok && !reply.WrongLeader {
			ck.prevLeader = server
			ck.prevIndex = reply.Index
			DPrintf("Get %v succeed by %v", key, reply.Index)
			return reply.Value
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	var args PutAppendArgs

	args.Key = key
	args.Value = value
	args.Op = op
	args.PrevIndex = ck.prevIndex
	args.Identifier = nrand()

	numServer := len(ck.servers)
	for i := 0; ; i = (i + 1) % numServer {
		var reply PutAppendReply
		server := (i + ck.prevLeader) % numServer
		ok := ck.servers[server].Call("KVServer.PutAppend", &args, &reply)
		if ok && !reply.WrongLeader {
			ck.prevLeader = server
			ck.prevIndex = reply.Index
			DPrintf("PutAppend %v: %v succeed by %d", key, value, reply.Index)
			return
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
