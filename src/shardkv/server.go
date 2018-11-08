package shardkv

// import "shardmaster"
import (
	"bytes"
	"fmt"
	"labgob"
	"labrpc"
	"log"
	"raft"
	"shardmaster"
	"sync"
	"time"
)

const Debug = 0

func (kv *ShardKV) debug(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(fmt.Sprintf("Server %2d %2d: ", kv.gid, kv.me)+format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type     string
	ClientID int64
	SeqNum   int

	Key   string
	Value string

	IsReconfig bool
	Config     shardmaster.Config
}

type Shard struct {
	mu        sync.Mutex
	data      map[string]string
	clientMap map[int64]int
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	// data map[string]string
	data   map[int]Shard
	config shardmaster.Config

	pendingConfigNum int
	mck              *shardmaster.Clerk
	regApplyMap      map[int]regApplyInfo
	killed           bool
}

type regApplyInfo struct {
	op Op
	ch chan Err
}

type State struct {
	Data   map[int]Shard
	Config shardmaster.Config
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
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
		kv.mu.Unlock()
		return
	}

	ch := make(chan Err, 1)
	kv.regApply(index, op, ch)
	kv.mu.Unlock()

	err := <-ch

	switch err {
	case OK:
		reply.WrongLeader = false
		kv.mu.Lock()
		reply.Value = kv.data[key2shard(op.Key)].data[op.Key]
		kv.mu.Unlock()
	case ErrWrongGroup:
		reply.WrongLeader = false
	case ErrLeaderChange:
		reply.WrongLeader = true
	}
	reply.Err = err
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
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

	ch := make(chan Err, 1)
	kv.regApply(index, op, ch)
	kv.mu.Unlock()

	err := <-ch

	switch err {
	case OK:
		reply.WrongLeader = false
		kv.mu.Lock()
		kv.mu.Unlock()
	case ErrWrongGroup:
		reply.WrongLeader = false
	case ErrLeaderChange:
		reply.WrongLeader = true
	}
	reply.Err = err
}

func (kv *ShardKV) regApply(index int, op Op, ch chan Err) {
	info, prs := kv.regApplyMap[index]
	if prs {
		info.ch <- ErrLeaderChange
		delete(kv.regApplyMap, index)
	}

	kv.regApplyMap[index] = regApplyInfo{op, ch}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	kv.killed = true
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.mck = shardmaster.MakeClerk(kv.masters)

	// Your initialization code here.
	kv.regApplyMap = make(map[int]regApplyInfo)
	kv.data = make(map[int]Shard)
	kv.killed = false

	go kv.waitApplyThread()
	go kv.watchLeaderChangeThread()
	go kv.watchConfigurationThread()

	return kv
}

func (kv *ShardKV) waitApplyThread() {
	for ap := range kv.applyCh {
		if kv.killed {
			break
		}

		if ap.ApplySnapshot {
			kv.applySnapshot(ap)
			continue
		}

		kv.mu.Lock()
		index := ap.CommandIndex
		info, prs := kv.regApplyMap[index]
		op := ap.Command.(Op)

		if op.IsReconfig {
			kv.applyConfig(op.Config)
			kv.mu.Unlock()
			continue
		}

		shard := key2shard(op.Key)
		if !kv.haveShard(shard) {
			if prs {
				if op.ClientID == info.op.ClientID && op.SeqNum == info.op.SeqNum {
					info.ch <- ErrWrongGroup
					delete(kv.regApplyMap, ap.CommandIndex)
				} else {
					info.ch <- ErrLeaderChange
					delete(kv.regApplyMap, ap.CommandIndex)
				}
			}
			kv.mu.Unlock()
			continue
		}

		if prs {
			if op.ClientID == info.op.ClientID && op.SeqNum == info.op.SeqNum {
				info.ch <- OK
				delete(kv.regApplyMap, ap.CommandIndex)
			} else {
				info.ch <- ErrLeaderChange
				delete(kv.regApplyMap, ap.CommandIndex)
			}
		}
		kv.applyOP(op, shard)

		if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() > kv.maxraftstate {
			kv.saveSnapshot(index)
		}
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) haveShard(shardNum int) bool {
	return kv.config.Shards[shardNum] == kv.gid
}

func (kv *ShardKV) saveSnapshot(index int) {
	state := State{
		Data:   kv.data,
		Config: kv.config,
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(state)
	kv.debug("saving snapshot")
	kv.rf.SaveSnapshot(w.Bytes(), index)
}

func (kv *ShardKV) applySnapshot(ap raft.ApplyMsg) {
	kv.mu.Lock()
	snapshot := ap.SnapshotState
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var state State
	if d.Decode(&state) != nil {
		panic("Decode Failed!")
	}

	kv.data = state.Data
	kv.config = state.Config
	for _, info := range kv.regApplyMap {
		info.ch <- ErrLeaderChange
		delete(kv.regApplyMap, ap.CommandIndex)
	}
	kv.mu.Unlock()
}

func (kv *ShardKV) applyOP(op Op, shardNum int) {
	shard := kv.data[shardNum]
	seq, prs := shard.clientMap[op.ClientID]
	if prs && seq >= op.SeqNum {
		kv.debug("detect duplicate!")
		return
	}
	switch op.Type {
	case "Put":
		shard.data[op.Key] = op.Value
	case "Append":
		v, prs := shard.data[op.Key]
		if !prs {
			shard.data[op.Key] = op.Value
		} else {
			shard.data[op.Key] = v + op.Value
		}
	case "Get":
	default:
		panic("unknown Op type")
	}
	shard.clientMap[op.ClientID] = op.SeqNum
}

// invalidate all pending requests when leader changes
// safe because duplicate detection
func (kv *ShardKV) watchLeaderChangeThread() {
	ch := kv.rf.GetLeaderChangeCh()
	for {
		if kv.killed {
			break
		}

		<-ch
		kv.mu.Lock()
		for index, info := range kv.regApplyMap {
			info.ch <- ErrLeaderChange
			delete(kv.regApplyMap, index)
		}
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) watchConfigurationThread() {
	for {
		if kv.killed {
			break
		}

		kv.mu.Lock()
		thisConfigNum := kv.config.Num
		kv.mu.Unlock()

		config := kv.mck.Query(thisConfigNum + 1)

		kv.mu.Lock()
		if config.Num == kv.config.Num+1 {
			kv.startConfig(config)
		}
		kv.mu.Unlock()
		time.Sleep(200)
	}
}

func (kv *ShardKV) startConfig(config shardmaster.Config) {
	kv.debug("started config %d", config.Num)
	op := Op{
		IsReconfig: true,
		Config:     config,
	}
	kv.rf.Start(op)
}

func (kv *ShardKV) applyConfig(config shardmaster.Config) {
	if config.Num <= kv.config.Num {
		return
	}
	kv.debug("current config is %v, applying config %v", kv.config.Num, config.Num)
	// applying
	for shardNum, gid := range config.Shards {
		if kv.gid == gid {
			kv.data[shardNum] = Shard{
				data:      make(map[string]string),
				clientMap: make(map[int64]int),
			}
		}
	}
	kv.config = config
}
