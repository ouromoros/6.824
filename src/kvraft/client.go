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
	nextSeqNum int
	id         int64
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
	ck.nextSeqNum = 0
	ck.id = nrand()
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
	args.SeqNum = ck.nextSeqNum
	args.ClientID = ck.id

	numServer := len(ck.servers)
	for {
		for i := 0; ; i = (i + 1) % numServer {
			var reply GetReply
			server := (i + ck.prevLeader) % numServer
			ok := ck.servers[server].Call("KVServer.Get", &args, &reply)
			if ok && !reply.WrongLeader {
				ck.prevLeader = server
				ck.nextSeqNum++
				return reply.Value
			}
		}
		time.Sleep(time.Millisecond * 500)
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
	args.ClientID = ck.id
	args.SeqNum = ck.nextSeqNum

	numServer := len(ck.servers)
	DPrintf("PutAppend %v: %v pending", key, value)
	for {
		for i := 0; ; i = (i + 1) % numServer {
			var reply PutAppendReply
			server := (i + ck.prevLeader) % numServer
			ok := ck.servers[server].Call("KVServer.PutAppend", &args, &reply)
			if ok && !reply.WrongLeader {
				ck.prevLeader = server
				ck.nextSeqNum++
				return
			}
		}
		time.Sleep(time.Millisecond * 500)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
