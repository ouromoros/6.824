package raftkv

import (
	"bytes"
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
	Type     string
	Key      string
	Value    string
	ClientID int64
	SeqNum   int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	regApplyMap map[int]regApplyInfo
	killed      bool
	clientMap   map[int64]int

	data map[string]string
}

type regApplyInfo struct {
	op Op
	ch chan bool
}

type State struct {
	ClientMap map[int64]int
	Data      map[string]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	op := Op{
		Type:     "Get",
		Key:      args.Key,
		Value:    "",
		ClientID: args.ClientID,
		SeqNum:   args.SeqNum,
	}

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = "Not Leader"
		kv.mu.Unlock()
		return
	}

	ch := make(chan bool)
	kv.regApply(index, op, ch)
	kv.mu.Unlock()

	success := <-ch

	if success {
		reply.WrongLeader = false
		kv.mu.Lock()
		reply.Value = kv.data[op.Key]
		kv.mu.Unlock()
	} else {
		reply.WrongLeader = true
		reply.Err = "Leader Change"
		return
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	op := Op{
		Type:     args.Op,
		Key:      args.Key,
		Value:    args.Value,
		ClientID: args.ClientID,
		SeqNum:   args.SeqNum,
	}

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = "Not Leader"
		kv.mu.Unlock()
		return
	}

	ch := make(chan bool, 1)
	kv.regApply(index, op, ch)
	kv.mu.Unlock()

	success := <-ch

	if success {
		reply.WrongLeader = false
	} else {
		reply.WrongLeader = true
		reply.Err = "Leader Change"
	}
}

func (kv *KVServer) regApply(index int, op Op, ch chan bool) {
	_, prs := kv.regApplyMap[index]
	if prs {
		kv.invalidatePendingRequest(index)
	}

	kv.regApplyMap[index] = regApplyInfo{op, ch}
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

	kv.applyCh = make(chan raft.ApplyMsg, 1)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.regApplyMap = make(map[int]regApplyInfo)
	kv.data = make(map[string]string)
	kv.killed = false
	kv.clientMap = make(map[int64]int)

	go kv.waitApplyThread()
	go kv.watchLeaderChangeThread()

	return kv
}

func (kv *KVServer) waitApplyThread() {
	for ap := range kv.applyCh {
		if kv.killed {
			break
		}
		if ap.ApplySnapshot {
			kv.mu.Lock()
			snapshot := ap.SnapshotState
			r := bytes.NewBuffer(snapshot)
			d := labgob.NewDecoder(r)
			var state State
			if d.Decode(&state) != nil {
				panic("Decode Failed!")
			}

			kv.clientMap = state.ClientMap
			kv.data = state.Data
			for i := range kv.regApplyMap {
				kv.invalidatePendingRequest(i)
			}
			kv.mu.Unlock()
			continue
		}

		kv.mu.Lock()
		index := ap.CommandIndex
		info, prs := kv.regApplyMap[index]
		if prs {
			if ap.Command.(Op) == info.op {
				info.ch <- true
				delete(kv.regApplyMap, ap.CommandIndex)
			} else {
				kv.invalidatePendingRequest(index)
			}
		}
		op := ap.Command.(Op)
		kv.applyOP(op)

		if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() > kv.maxraftstate {
			state := State{
				ClientMap: kv.clientMap,
				Data:      kv.data,
			}
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			e.Encode(state)
			DPrintf("Saving snapshot")
			kv.rf.SaveSnapshot(w.Bytes(), index)
		}
		kv.mu.Unlock()
	}
}

func (kv *KVServer) applyOP(op Op) {
	seq, prs := kv.clientMap[op.ClientID]
	if prs && seq >= op.SeqNum {
		DPrintf("Detect duplicate!")
		return
	}

	switch op.Type {
	case "Put":
		kv.data[op.Key] = op.Value
	case "Append":
		v, prs := kv.data[op.Key]
		if !prs {
			kv.data[op.Key] = op.Value
		} else {
			kv.data[op.Key] = v + op.Value
		}
	case "Get":
	default:
		panic("unknown Op type")
	}
	kv.clientMap[op.ClientID] = op.SeqNum
}

// invalidate all pending requests when leader changes
// safe because duplicate detection
func (kv *KVServer) watchLeaderChangeThread() {
	ch := kv.rf.GetLeaderChangeCh()
	for {
		<-ch
		kv.mu.Lock()
		for i := range kv.regApplyMap {
			kv.invalidatePendingRequest(i)
		}
		kv.mu.Unlock()
	}
}

func (kv *KVServer) invalidatePendingRequest(i int) {
	kv.regApplyMap[i].ch <- false
	delete(kv.regApplyMap, i)
}
