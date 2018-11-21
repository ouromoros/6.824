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

func (kv *ShardKV) debug1(format string, a ...interface{}) (n int, err error) {
	if Debug > 1 {
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
	data          map[int]*Shard
	currentConfig shardmaster.Config
	pending       bool

	// configs						map[int]shardmaster.Config
	configs     sync.Map
	mck         *shardmaster.Clerk
	regApplyMap map[int]regApplyInfo
	killed      bool
}

type regApplyInfo struct {
	op Op
	ch chan result
}

type result struct {
	err   Err
	value string
}

type State struct {
	Data    map[int]*Shard
	Config  shardmaster.Config
	Pending bool
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

	if args.ConfigNum != kv.currentConfig.Num {
		kv.debug("request confNum: %v, current: %v", args.ConfigNum, kv.currentConfig.Num)
		kv.tryUpdateConfig()
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		kv.mu.Unlock()
		return
	}
	kv.debug("started %v", index)

	ch := make(chan result, 1)
	kv.regApply(index, op, ch)
	kv.mu.Unlock()

	var r result
out:
	for {
		select {
		case r = <-ch:
			break out
		case <-time.After(1000 * time.Millisecond):
			kv.debug("index %v I'm stuck", index)
		}
	}
	err := r.err

	kv.debug("%v", err)
	switch err {
	case OK:
		reply.WrongLeader = false
		reply.Value = r.value
	case ErrWrongGroup:
		reply.WrongLeader = false
	case ErrLeaderChange:
		reply.WrongLeader = true
	case ErrShardNotReady:
		reply.WrongLeader = false
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

	if args.ConfigNum != kv.currentConfig.Num {
		kv.debug("request confNum: %v, current: %v", args.ConfigNum, kv.currentConfig.Num)
		kv.tryUpdateConfig()
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		kv.mu.Unlock()
		return
	}
	kv.debug("started %v", index)

	ch := make(chan result, 1)
	kv.regApply(index, op, ch)
	kv.mu.Unlock()

	var r result
out:
	for {
		select {
		case r = <-ch:
			break out
		case <-time.After(1000 * time.Millisecond):
			kv.debug("index %v I'm stuck", index)
		}
	}
	err := r.err

	kv.debug("%v", err)
	switch err {
	case OK:
		reply.WrongLeader = false
	case ErrWrongGroup:
		reply.WrongLeader = false
	case ErrLeaderChange:
		reply.WrongLeader = true
	case ErrShardNotReady:
		reply.WrongLeader = false
	}
	reply.Err = err
}

func (kv *ShardKV) regApply(index int, op Op, ch chan result) {
	info, prs := kv.regApplyMap[index]
	if prs {
		info.ch <- result{err: ErrLeaderChange}
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
	kv.mu.Lock()
	for index, info := range kv.regApplyMap {
		info.ch <- result{err: ErrLeaderChange}
		delete(kv.regApplyMap, index)
	}
	kv.mu.Unlock()
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

	kv.applyCh = make(chan raft.ApplyMsg, 1)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.mck = shardmaster.MakeClerk(kv.masters)

	// Your initialization code here.
	// kv.configs = make(map[int]shardmaster.Config)
	kv.regApplyMap = make(map[int]regApplyInfo)
	kv.data = make(map[int]*Shard)
	// kv.currentConfig = kv.getConfig(0)
	kv.killed = false

	go kv.waitApplyThread()
	go kv.watchLeaderChangeThread()
	go kv.watchConfigThread()
	go kv.cleanOldShardThread()
	if Debug > 0 {
		go kv.printDebugThread()
	}

	return kv
}

func (kv *ShardKV) waitApplyThread() {
	for ap := range kv.applyCh {
		if kv.killed {
			break
		}

		kv.mu.Lock()
		if ap.ApplySnapshot {
			kv.applySnapshot(ap)
			kv.mu.Unlock()
			continue
		}

		index := ap.CommandIndex
		info, prs := kv.regApplyMap[index]
		op := ap.Command.(Op)

		if op.IsReconfig || op.IsShard {
			if prs {
				info.ch <- result{err: ErrLeaderChange}
				delete(kv.regApplyMap, index)
			}
		}

		if op.IsReconfig {
			kv.applyConfig(op.Config)
			kv.mu.Unlock()
			continue
		}

		if op.IsShard {
			kv.debug("apply shard %v", op.Shard.ShardNum)
			kv.applyShard(op.Shard)
			if kv.pending && kv.shardsAllPresent() {
				kv.pending = false
				kv.tryUpdateConfig()
			}
			if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() > kv.maxraftstate {
				kv.saveSnapshot(index)
			}
			kv.mu.Unlock()
			continue
		}

		shard := key2shard(op.Key)
		if kv.currentConfig.Shards[shard] != kv.gid {
			if prs {
				info.ch <- result{err: ErrWrongGroup}
				delete(kv.regApplyMap, index)
			}
			kv.mu.Unlock()
			continue
		}
		if !kv.data[shard].Present {
			if prs {
				info.ch <- result{err: ErrShardNotReady}
				delete(kv.regApplyMap, index)
			}
			kv.mu.Unlock()
			continue
		}

		// just reject requests when shard not ready
		r := kv.applyOP(op, shard)
		kv.debug("applied %v", index)
		if prs {
			if op.ClientID != info.op.ClientID || op.SeqNum != info.op.SeqNum {
				info.ch <- result{err: ErrLeaderChange}
				delete(kv.regApplyMap, index)
			} else {
				info.ch <- result{err: OK, value: r}
				delete(kv.regApplyMap, index)
			}
		}

		if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() > kv.maxraftstate {
			kv.saveSnapshot(index)
		}
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) applyShard(shard Shard) {
	shardNum := shard.ShardNum
	assert(shard.ConfNum == kv.currentConfig.Num)
	if _, prs := kv.data[shardNum]; !prs {
		return
	}
	if kv.data[shardNum].Present {
		return
	}

	copy := shardCopy(&shard)
	kv.data[shardNum] = &copy
}

func (kv *ShardKV) shardsAllPresent() bool {
	for _, shard := range kv.data {
		if !shard.Present {
			return false
		}
	}
	return true
}

func (kv *ShardKV) saveSnapshot(index int) {
	state := State{
		Data:    kv.data,
		Config:  kv.currentConfig,
		Pending: kv.pending,
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(state)
	kv.debug("saving snapshot")
	kv.rf.SaveSnapshot(w.Bytes(), index)
}

func (kv *ShardKV) applySnapshot(ap raft.ApplyMsg) {
	snapshot := ap.SnapshotState
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var state State
	if d.Decode(&state) != nil {
		panic("Decode Failed!")
	}

	kv.data = state.Data
	kv.currentConfig = state.Config
	kv.pending = state.Pending
	for _, info := range kv.regApplyMap {
		info.ch <- result{err: ErrLeaderChange}
		delete(kv.regApplyMap, ap.CommandIndex)
	}
	kv.tryRequestShards()
}

func (kv *ShardKV) applyOP(op Op, shardNum int) string {
	shard := kv.data[shardNum]
	seq, prs := shard.ClientMap[op.ClientID]
	result := ""
	if prs && seq >= op.SeqNum {
		kv.debug("detect duplicate!")
		if op.Type == "Get" {
			result = shard.Data[op.Key]
		}
		return result
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
		result = shard.Data[op.Key]
	default:
		panic("unknown Op type")
	}
	shard.ClientMap[op.ClientID] = op.SeqNum
	return result
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
			info.ch <- result{err: ErrLeaderChange}
			delete(kv.regApplyMap, index)
		}
		kv.mu.Unlock()
	}
}

// periodically check if there is new config needed to be fetched
func (kv *ShardKV) watchConfigThread() {
	for {
		if kv.killed {
			break
		}
		kv.tryUpdateConfig()
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) tryUpdateConfig() {
	if !kv.pending {
		thisConfigNum := kv.currentConfig.Num
		go func() {
			config := kv.getConfig(thisConfigNum + 1)
			kv.mu.Lock()
			if config.Num == kv.currentConfig.Num+1 {
				op := Op{
					IsReconfig: true,
					Config:     configCopy(config),
				}
				kv.rf.Start(op)
			}
			kv.mu.Unlock()
		}()
	}
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
	if config.Num <= kv.currentConfig.Num {
		return
	}
	kv.debug("applying config %v", config.Num)
	assert(config.Num == kv.currentConfig.Num+1)
	assert(kv.shardsAllPresent())

	// Process newly added and unchanged shards
	for shardNum, gid := range config.Shards {
		if kv.gid != gid {
			continue
		}
		if kv.currentConfig.Shards[shardNum] == 0 {
			kv.data[shardNum] = &Shard{
				Data:      make(map[string]string),
				ClientMap: make(map[int64]int),
				ConfNum:   config.Num,
				ShardNum:  shardNum,
				Present:   true,
				Owner:     kv.gid,
			}
		} else if kv.currentConfig.Shards[shardNum] == gid {
			kv.data[shardNum].ConfNum = config.Num
		} else {
			// go kv.requestShard(shardNum, kv.config.Num)
			if shard, prs := kv.data[shardNum]; prs {
				shard.Present = false
			} else {
				kv.data[shardNum] = &Shard{
					ShardNum: shardNum,
					ConfNum:  config.Num,
					Present:  false,
					Owner:    kv.gid,
				}
			}
		}
	}
	// Handle old shards
	// for shardNum, gid := range thisConfig.Shards {
	// 	if kv.gid == gid && config.Shards[shardNum] != gid {

	// 	}
	// }
	kv.currentConfig = config
	kv.pending = true
	kv.tryRequestShards()
}

// A thread is not necessary because we know exactly when there might be new shards.
// Only call this function when (1) the server recovers from snapshot and (2) config changes
// check present flag in shards and try to fetch if needed
func (kv *ShardKV) tryRequestShards() {
	confNum := kv.currentConfig.Num
	for shardNum, shard := range kv.data {
		if !shard.Present {
			num := shardNum
			assert(kv.currentConfig.Shards[shardNum] == kv.gid)
			go kv.requestShard(num, confNum)
		}
	}
	if kv.pending && kv.shardsAllPresent() {
		kv.pending = false
		kv.tryUpdateConfig()
	}
}

// after this function finishes, shard with shardNum should be present
// halt if config changes in the process since tryReqeustShards will be called again
func (kv *ShardKV) requestShard(shardNum int, confNum int) {
	var args RequestShardArgs
	var shard Shard
	config := kv.getConfig(confNum - 1)
	servers := config.Groups[config.Shards[shardNum]]
	args.ShardNum = shardNum
	args.ConfigNum = confNum - 1
	kv.debug("requesting Shard %v from %v", shardNum, config.Shards[shardNum])
loop:
	// try to get the shard
	for {
		if kv.killed {
			break
		}
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
		kv.debug("requesting Shard %v from %v failed! retrying...", shardNum, config.Shards[shardNum])
		kv.mu.Lock()
		if _, prs := kv.data[shardNum]; !prs {
			kv.mu.Unlock()
			return
		}
		if kv.currentConfig.Num != confNum || kv.data[shardNum].Present {
			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}

	shard.ConfNum = confNum
	shard.Owner = kv.gid
	shard.Present = true
	// commit the shard to log
	kv.debug("commiting shard %v", shardNum)
	for {
		if kv.killed {
			break
		}
		kv.mu.Lock()
		if _, prs := kv.data[shardNum]; !prs {
			kv.mu.Unlock()
			return
		}
		if kv.currentConfig.Num != confNum || kv.data[shardNum].Present {
			kv.mu.Unlock()
			break
		}
		kv.rf.Start(Op{
			IsShard: true,
			Shard:   shard,
		})
		kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
	kv.debug("commited shard %v", shardNum)
}

// return the requested shard if it exists
func (kv *ShardKV) RequestShard(args *RequestShardArgs, reply *RequestShardReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	shard, prs := kv.data[args.ShardNum]
	if !prs {
		kv.debug("shard not exist")
		reply.Success = false
		return
	}
	if args.ConfigNum == kv.currentConfig.Num {
		kv.debug("config not update")
		kv.tryUpdateConfig()
		reply.Success = false
		return
	}
	if shard.ConfNum != args.ConfigNum {
		kv.debug("%v %v", args, shard)
		reply.Success = false
		return
	}

	reply.Success = true
	reply.Data = shardCopy(shard)
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
		ShardNum:  shard.ShardNum,
		Present:   true,
		Owner:     shard.Owner,
	}
}

func (kv *ShardKV) GetConfigNum(args *GetConfigNumArgs, reply *GetConfigNumReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.pending {
		reply.Num = kv.currentConfig.Num - 1
	} else {
		reply.Num = kv.currentConfig.Num
	}
}

// can be further optimized (add the configNum to be compared here)
func (kv *ShardKV) compareConfigNum(servers []string, num int) bool {
	var args GetConfigNumArgs
	ch := make(chan int, len(servers))
	for _, s := range servers {
		srv := kv.make_end(s)
		var reply GetConfigNumReply
		go func() {
			ok := srv.Call("ShardKV.GetConfigNum", &args, &reply)
			if ok {
				ch <- reply.Num
			} else {
				ch <- -1
			}
		}()
	}
	for i := 0; i < len(servers); i++ {
		n := <-ch
		if n > num {
			return true
		}
	}
	return false
}

func (kv *ShardKV) getConfig(num int) shardmaster.Config {
	value, prs := kv.configs.Load(num)
	if prs {
		return value.(shardmaster.Config)
	}
	config := kv.mck.Query(num)
	if config.Num == num {
		kv.configs.Store(num, config)
	}
	return config
}

// periodically check the config number of other groups, garbage clean safe shards
func (kv *ShardKV) cleanOldShardThread() {
	for {
		if kv.killed {
			break
		}
		kv.mu.Lock()
		for shardNum, shard := range kv.data {
			if shard.ConfNum == kv.currentConfig.Num || !shard.Present {
				continue
			}
			thisConfigNum := shard.ConfNum
			thisShardNum := shardNum
			go func() {
				config := kv.getConfig(thisConfigNum + 1)
				safeToDelete := kv.compareConfigNum(config.Groups[config.Shards[thisShardNum]], thisConfigNum)
				kv.mu.Lock()
				defer kv.mu.Unlock()
				shard, prs := kv.data[thisShardNum]
				if !prs || shard.ConfNum != thisConfigNum || !shard.Present {
					return
				}
				if safeToDelete {
					kv.debug("delete %v", thisShardNum)
					delete(kv.data, thisShardNum)
				}
			}()
		}
		kv.mu.Unlock()

		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) printDebugThread() {
	for {
		if kv.killed {
			break
		}
		kv.mu.Lock()
		kv.debug("Info:")
		print("Shards: ")
		for _, shard := range kv.data {
			if shard.Present && shard.ConfNum == kv.currentConfig.Num {
				print(shard.ShardNum, " ")
			}
		}
		print("\n")
		print("Conf: ")
		print(kv.currentConfig.Num, " ", kv.pending, "\n")
		kv.debug("%v", kv.regApplyMap)
		kv.mu.Unlock()
		time.Sleep(time.Millisecond * 1000)
	}
}
