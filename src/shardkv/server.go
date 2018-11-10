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

func assert(cond bool) {
	if !cond {
		panic("assert failed!")
	}
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

	IsShard bool
	Shard   Shard
}

type Shard struct {
	Data      map[string]string
	ClientMap map[int64]int
	ConfNum   int
	ShardNum  int
	Present   bool
	Owner     int
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
	data      map[int]*Shard
	shardMuts map[int]*sync.Mutex
	config    shardmaster.Config
	pending   bool

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
	Data    map[int]*Shard
	Config  shardmaster.Config
	pending bool
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
		shardNum := key2shard(op.Key)
		for {
			kv.shardMuts[shardNum].Lock()
			if kv.data[shardNum].Present {
				reply.Value = kv.data[shardNum].Data[op.Key]
				break
			}
			kv.shardMuts[shardNum].Unlock()
			time.Sleep(100 * time.Millisecond)
		}
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
		shardNum := key2shard(op.Key)
		for {
			kv.shardMuts[shardNum].Lock()
			if kv.data[shardNum].Present {
				break
			}
			kv.shardMuts[shardNum].Unlock()
			time.Sleep(100 * time.Millisecond)
		}
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
	kv.data = make(map[int]*Shard)
	kv.killed = false

	kv.initConfig()

	go kv.waitApplyThread()
	go kv.watchLeaderChangeThread()
	go kv.watchConfigThread()
	go kv.fetchNewShardThread()
	go kv.cleanOldShardThread()

	return kv
}

// get latest config and initialize
func (kv *ShardKV) initConfig() {

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

		if op.IsShard {
			kv.applyShard(op.Shard)
			if kv.pending && !kv.configPending() {
				kv.pending = false
				kv.saveSnapshot(index)
			}
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

func (kv *ShardKV) applyShard(shard Shard) {
	shardNum := shard.ShardNum
	kv.shardMuts[shardNum].Lock()
	if kv.data[shardNum].Present {
		kv.shardMuts[shardNum].Unlock()
		return
	}

	defer kv.shardMuts[shard.ShardNum].Unlock()
	copy := shardCopy(&shard)
	copy.Owner = kv.gid
	kv.data[shard.ShardNum] = &copy
}

func (kv *ShardKV) configPending() bool {
	for _, shard := range kv.data {
		if !shard.Present {
			return true
		}
	}
	return false
}

func (kv *ShardKV) haveShard(shardNum int) bool {
	return kv.config.Shards[shardNum] == kv.gid
}

func (kv *ShardKV) saveSnapshot(index int) {
	state := State{
		Data:    kv.data,
		Config:  kv.config,
		pending: kv.pending,
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
	kv.pending = state.pending
	for _, info := range kv.regApplyMap {
		info.ch <- ErrLeaderChange
		delete(kv.regApplyMap, ap.CommandIndex)
	}
	kv.mu.Unlock()
}

func (kv *ShardKV) applyOP(op Op, shardNum int) {
	shard := kv.data[shardNum]
	seq, prs := shard.ClientMap[op.ClientID]
	if prs && seq >= op.SeqNum {
		kv.debug("detect duplicate!")
		return
	}
	switch op.Type {
	case "Put":
		shard.Data[op.Key] = op.Value
	case "Append":
		v, prs := shard.Data[op.Key]
		if !prs {
			shard.Data[op.Key] = op.Value
		} else {
			shard.Data[op.Key] = v + op.Value
		}
	case "Get":
	default:
		panic("unknown Op type")
	}
	shard.ClientMap[op.ClientID] = op.SeqNum
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

func (kv *ShardKV) watchConfigThread() {
	for {
		if kv.killed {
			break
		}

		kv.mu.Lock()
		thisConfigNum := kv.config.Num
		kv.mu.Unlock()

		// overhead here, send duplicate RPCs when pending
		config := kv.getConfig(thisConfigNum + 1)
		kv.mu.Lock()
		if !kv.pending && config.Num == kv.config.Num+1 {
			kv.startConfig(config)
		}
		kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) startConfig(config shardmaster.Config) {
	kv.debug("started config %d", config.Num)
	op := Op{
		IsReconfig: true,
		Config:     configCopy(config),
	}
	kv.rf.Start(op)
}

func configCopy(config shardmaster.Config) shardmaster.Config {
	g := make(map[int][]string)
	for k, v := range config.Groups {
		g[k] = v
	}
	s := config.Shards
	return shardmaster.Config{
		Num:    config.Num,
		Shards: s,
		Groups: g,
	}
}

func (kv *ShardKV) applyConfig(config shardmaster.Config) {
	if config.Num <= kv.config.Num {
		return
	}
	kv.debug("applying config %v", config.Num)
	assert(config.Num == kv.config.Num+1)

	thisConfig := kv.config
	// Process newly added and unchanged shards
	for shardNum, gid := range config.Shards {
		if kv.gid != gid {
			continue
		}
		if thisConfig.Shards[shardNum] == 0 {
			kv.data[shardNum] = &Shard{
				Data:      make(map[string]string),
				ClientMap: make(map[int64]int),
				ConfNum:   config.Num,
				ShardNum:  shardNum,
				Present:   true,
				Owner:     kv.gid,
			}
			kv.shardMuts[shardNum] = &sync.Mutex{}
		} else if thisConfig.Shards[shardNum] == gid {
			kv.data[shardNum].ConfNum = config.Num
		} else {
			// go kv.requestShard(shardNum, kv.config.Num, kv.config.Groups[gid])
			if shard, prs := kv.data[shardNum]; prs {
				kv.shardMuts[shardNum].Lock()
				shard.Present = false
				kv.shardMuts[shardNum].Unlock()
			} else {
				kv.data[shardNum] = &Shard{Present: false}
				kv.shardMuts[shardNum] = &sync.Mutex{}
			}
		}
	}
	// // Handle old shards
	// for shardNum, gid := range thisConfig.Shards {
	// 	if kv.gid == gid && config.Shards[shardNum] != gid {

	// 	}
	// }
	kv.config = config
	kv.pending = true
}

// check present flag in shards and try to fetch if needed
func (kv *ShardKV) fetchNewShardThread() {

}

// TODO
func (kv *ShardKV) requestShard(shardNum int, confNum int, servers []string) {
	var args RequestShardArgs
	var shard Shard
loop:
	for {
		for _, s := range servers {
			srv := kv.make_end(s)
			var reply RequestShardReply
			ok := srv.Call("ShardKV.RequestShard", &args, &reply)
			if !ok {
				continue
			}
			if reply.Success {
				shard = reply.Data
				break loop
			}
		}
		// It's possible the Shard is committed through log
		kv.shardMuts[shardNum].Lock()
		if kv.data[shardNum].Present {
			kv.shardMuts[shardNum].Unlock()
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// commit the shard to log
	for {
		kv.shardMuts[shardNum].Lock()
		if kv.data[shardNum].Present {
			kv.shardMuts[shardNum].Unlock()
			break
		}
		kv.rf.Start(Op{
			IsShard: true,
			Shard:   shard,
		})
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) RequestShard(args *RequestShardArgs, reply *RequestShardReply) {
	// kv.shardMuts[args.shardNum].Lock()
	// defer kv.shardMuts[args.shardNum].Unlock()
	// bugs here
	kv.mu.Lock()
	shardMut, prs := kv.shardMuts[args.ShardNum]
	if !prs {
		reply.Success = false
		kv.mu.Unlock()
		return
	}
	shardMut.Lock()
	if !kv.data[args.ShardNum].Present || kv.data[args.ShardNum].ConfNum != args.ConfigNum {
		reply.Success = false
		shardMut.Unlock()
		kv.mu.Unlock()
		return
	}

	reply.Success = true
	reply.Data = shardCopy(kv.data[args.ShardNum])
	shardMut.Unlock()
	kv.mu.Unlock()
}

func shardCopy(shard *Shard) Shard {
	data := make(map[string]string)
	clientMap := make(map[int64]int)
	for k, v := range shard.Data {
		data[k] = v
	}
	for k, v := range shard.ClientMap {
		clientMap[k] = v
	}
	return Shard{
		Data:      data,
		ClientMap: clientMap,
		ConfNum:   shard.ConfNum,
	}
}

func (kv *ShardKV) GetConfigNum(args *GetConfigNumArgs, reply *GetConfigNumReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.pending {
		reply.Num = kv.config.Num - 1
	} else {
		reply.Num = kv.config.Num
	}
}

func (kv *ShardKV) getConfigNum(servers []string) int {
	var args GetConfigNumArgs
	n := -1
	// can add concurrency here
	for _, s := range servers {
		srv := kv.make_end(s)
		var reply GetConfigNumReply
		ok := srv.Call("ShardKV.GetConfigNum", &args, &reply)
		if ok && reply.Num > n {
			n = reply.Num
		}
	}
	return n
}

// TODO: Add cache here
func (kv *ShardKV) getConfig(num int) shardmaster.Config {
	return kv.mck.Query(num)
}

// periodically check the config number of other groups, garbage clean safe shards
func (kv *ShardKV) cleanOldShardThread() {
	// TODO: very unefficient
	// var confStatus map[int]int
	// var mut sync.Mutex

	for {
		if kv.killed {
			break
		}
		kv.mu.Lock()
		for shardNum, shard := range kv.data {
			if shard.ConfNum == kv.config.Num {
				continue
			}
			confNum := kv.getConfigNum(kv.getConfig(shard.ConfNum).Groups[shard.Owner])
			if confNum >= shard.ConfNum {
				delete(kv.data, shardNum)
				delete(kv.shardMuts, shardNum)
			}
		}
		kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}
