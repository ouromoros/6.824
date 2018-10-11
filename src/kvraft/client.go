package raftkv

import (
	"crypto/rand"
	"labrpc"
	"math/big"
	"time"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	prevLeader int
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

	// You will have to modify this function.
	DPrintf("Client sending Get")
	var args GetArgs

	args.Key = key
	args.Identifier = nrand()

	numServer := len(ck.servers)
	for i := 0; ; i = (i + 1) % numServer {
		var reply GetReply
		server := (i + ck.prevLeader) % numServer
		ok := ck.servers[server].Call("KVServer.Get", &args, &reply)
		if !ok || reply.WrongLeader {
			DPrintf("Client send Get to Server %d fail", server)
			continue
		} else {
			DPrintf("Client send Get to Server %d succeed", server)
			ck.prevLeader = server
			return reply.Value
		}

		if i == numServer - 1 {
			time.Sleep(time.Duration(500) * time.Millisecond)
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
	// You will have to modify this function.
	DPrintf("Client sending PutAppend")
	var args PutAppendArgs

	args.Key = key
	args.Value = value
	args.Op = op
	args.Identifier = nrand()

	numServer := len(ck.servers)
	for i := 0; ; i = (i + 1) % numServer {
		var reply PutAppendReply
		server := (i + ck.prevLeader) % numServer
		ok := ck.servers[server].Call("KVServer.PutAppend", &args, &reply)
		if !ok || reply.WrongLeader {
			DPrintf("Client send PutAppend to Server %d fail", server)
			continue
		} else {
			DPrintf("Client send PutAppend to Server %d succeed", server)
			ck.prevLeader = server
			return
		}
		if i == numServer - 1 {
			time.Sleep(time.Duration(500) * time.Millisecond)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
