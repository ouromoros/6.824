package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type       string
	Key        string
	Value      string
	Identifier int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	registerApply map[int]chan raft.ApplyMsg
	killed bool

	data map[string]string

}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	op := Op{
		Type:  "Get",
		Key:   args.Key,
		Value: "",
	}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = "Not Leader"
		kv.mu.Unlock()
		return
	}

	DPrintf("Server %d started Get on %d", kv.me, index)

	ch := make(chan raft.ApplyMsg)
	kv.registerApply[index] = ch
	kv.mu.Unlock()

	applyMsg := <-ch

	if op == applyMsg.Command.(Op) {
		reply.WrongLeader = false
		kv.mu.Lock()
		reply.Value = kv.data[op.Key]
		kv.mu.Unlock()
	} else {
		reply.WrongLeader = true
		reply.Err = "Leader Change"
	}
	DPrintf("Server %d finished Get", kv.me)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	op := Op{
		Type:       args.Op,
		Key:        args.Key,
		Value:      args.Value,
		Identifier: args.Identifier,
	}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = "Not Leader"
		kv.mu.Unlock()
		return
	}

	DPrintf("Server %d started PutAppend on %d", kv.me, index)

	ch := make(chan raft.ApplyMsg)
	kv.registerApply[index] = ch
	kv.mu.Unlock()

	applyMsg := <-ch

	if op == applyMsg.Command.(Op) {
		reply.WrongLeader = false
		kv.mu.Lock()
		switch op.Type {
		case "Put": kv.data[op.Key] = op.Value
		case "Append":
			v, prs := kv.data[op.Key]
			if !prs {
				kv.data[op.Key] = op.Value
			} else {
				kv.data[op.Key] = v + op.Value
			}
		default: panic("invalid command")
		}
		kv.mu.Unlock()
	} else {
		reply.WrongLeader = true
		reply.Err = "Leader Change"
	}
	DPrintf("Server %d finished PutAppend", kv.me)
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	kv.killed = true
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.registerApply = make(map[int]chan raft.ApplyMsg)
	kv.data = make(map[string]string)
	kv.killed = false
	go kv.waitApplyThread()

	return kv
}

func (kv *KVServer) waitApplyThread() {
	for ap := range kv.applyCh {
		if kv.killed { break }
		kv.mu.Lock()
		ch, prs := kv.registerApply[ap.CommandIndex]
		if prs {
			ch <- ap
			close(ch)
		}
		kv.mu.Unlock()
	}
}
