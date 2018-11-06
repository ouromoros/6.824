package shardmaster

import "raft"
import "labrpc"
import "sync"
import "labgob"
import "sort"
import "log"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	regApplyMap map[int]regApplyInfo
	killed      bool
	clientMap   map[int64]int

	configs []Config // indexed by config num
}

type regApplyInfo struct {
	op Op
	ch chan bool
}

type Op struct {
	// Your data here.
	Type     string
	ClientID int64
	SeqNum   int

	// Join args
	Servers map[int][]string
	// Leave args
	GIDS []int
	// Move args
	Shard int
	GID   int
	// Query args
	Num int
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	sm.mu.Lock()
	op := Op{
		Type:     "Join",
		Servers:  args.Servers,
		ClientID: args.ClientID,
		SeqNum:   args.SeqNum,
	}

	index, _, isLeader := sm.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = "Not Leader"
		sm.mu.Unlock()
		return
	}

	ch := make(chan bool, 1)
	sm.regApply(index, op, ch)
	sm.mu.Unlock()

	success := <-ch

	if success {
		reply.WrongLeader = false
	} else {
		reply.WrongLeader = true
		reply.Err = "Leader Change"
	}
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	sm.mu.Lock()
	op := Op{
		Type:     "Leave",
		GIDS:     args.GIDs,
		ClientID: args.ClientID,
		SeqNum:   args.SeqNum,
	}

	index, _, isLeader := sm.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = "Not Leader"
		sm.mu.Unlock()
		return
	}

	ch := make(chan bool, 1)
	sm.regApply(index, op, ch)
	sm.mu.Unlock()

	success := <-ch

	if success {
		reply.WrongLeader = false
	} else {
		reply.WrongLeader = true
		reply.Err = "Leader Change"
	}
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	sm.mu.Lock()
	op := Op{
		Type:     "Move",
		Shard:    args.Shard,
		GID:      args.GID,
		ClientID: args.ClientID,
		SeqNum:   args.SeqNum,
	}

	index, _, isLeader := sm.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = "Not Leader"
		sm.mu.Unlock()
		return
	}

	ch := make(chan bool, 1)
	sm.regApply(index, op, ch)
	sm.mu.Unlock()

	success := <-ch

	if success {
		reply.WrongLeader = false
	} else {
		reply.WrongLeader = true
		reply.Err = "Leader Change"
	}
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	sm.mu.Lock()
	op := Op{
		Type:     "Query",
		Num:      args.Num,
		ClientID: args.ClientID,
		SeqNum:   args.SeqNum,
	}

	index, _, isLeader := sm.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = "Not Leader"
		sm.mu.Unlock()
		return
	}

	ch := make(chan bool, 1)
	sm.regApply(index, op, ch)
	sm.mu.Unlock()

	success := <-ch

	if success {
		sm.mu.Lock()
		if args.Num >= len(sm.configs) || args.Num == -1 {
			reply.Config = sm.configs[len(sm.configs)-1]
		} else {
			reply.Config = sm.configs[args.Num]
		}
		sm.mu.Unlock()
		reply.WrongLeader = false
	} else {
		reply.WrongLeader = true
		reply.Err = "Leader Change"
	}
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
	sm.killed = true
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

func (sm *ShardMaster) regApply(index int, op Op, ch chan bool) {
	_, prs := sm.regApplyMap[index]
	if prs {
		sm.invalidatePendingRequest(index)
	}

	sm.regApplyMap[index] = regApplyInfo{op, ch}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.regApplyMap = make(map[int]regApplyInfo)
	sm.killed = false
	sm.clientMap = make(map[int64]int)
	sm.configs = make([]Config, 0)
	sm.configs = append(sm.configs, Config{
		Num:    0,
		Shards: [NShards]int{},
		Groups: make(map[int][]string),
	})

	go sm.waitApplyThread()
	go sm.watchLeaderChangeThread()

	return sm
}

func (sm *ShardMaster) waitApplyThread() {
	for ap := range sm.applyCh {
		if sm.killed {
			break
		}
		// if ap.ApplySnapshot {
		// 	sm.mu.Lock()
		// 	snapshot := ap.SnapshotState
		// 	r := bytes.NewBuffer(snapshot)
		// 	d := labgob.NewDecoder(r)
		// 	var state State
		// 	if d.Decode(&state) != nil {
		// 		panic("Decode Failed!")
		// 	}

		// 	sm.clientMap = state.ClientMap
		// 	sm.data = state.Data
		// 	for i := range kv.regApplyMap {
		// 		sm.invalidatePendingRequest(i)
		// 	}
		// 	sm.mu.Unlock()
		// 	continue
		// }

		sm.mu.Lock()
		op := ap.Command.(Op)
		index := ap.CommandIndex
		info, prs := sm.regApplyMap[index]
		if prs {
			if op.ClientID == info.op.ClientID && op.SeqNum == info.op.SeqNum {
				info.ch <- true
				delete(sm.regApplyMap, ap.CommandIndex)
			} else {
				sm.invalidatePendingRequest(index)
			}
		}
		sm.applyOP(op)

		// if sm.maxraftstate != -1 && sm.rf.GetRaftStateSize() > sm.maxraftstate {
		// 	state := State{
		// 		ClientMap: sm.clientMap,
		// 		Data:      sm.data,
		// 	}
		// 	w := new(bytes.Buffer)
		// 	e := labgob.NewEncoder(w)
		// 	e.Encode(state)
		// 	DPrintf("Saving snapshot")
		// 	sm.rf.SaveSnapshot(w.Bytes(), index)
		// }
		sm.mu.Unlock()
	}
}

func (sm *ShardMaster) applyOP(op Op) {
	seq, prs := sm.clientMap[op.ClientID]
	if prs && seq >= op.SeqNum {
		// DPrintf("Detect duplicate!")
		return
	}

	lastConf := sm.configs[len(sm.configs)-1]
	switch op.Type {
	case "Join":
		newConf := join(lastConf, op.Servers)
		sm.configs = append(sm.configs, newConf)
	case "Leave":
		newConf := leave(lastConf, op.GIDS)
		sm.configs = append(sm.configs, newConf)
	case "Move":
		newConf := move(lastConf, op.Shard, op.GID)
		sm.configs = append(sm.configs, newConf)
	case "Query":
	}
	sm.clientMap[op.ClientID] = op.SeqNum
}

// invalidate all pending requests when leader changes
// safe because duplicate detection
func (sm *ShardMaster) watchLeaderChangeThread() {
	ch := sm.rf.GetLeaderChangeCh()
	for {
		<-ch
		sm.mu.Lock()
		for i := range sm.regApplyMap {
			sm.invalidatePendingRequest(i)
		}
		sm.mu.Unlock()
	}
}

func (sm *ShardMaster) invalidatePendingRequest(i int) {
	sm.regApplyMap[i].ch <- false
	delete(sm.regApplyMap, i)
}

func join(conf Config, servers map[int][]string) Config {
	groups := copyOfGroups(conf.Groups)
	for gid, server := range servers {
		_, prs := groups[gid]
		assert(!prs)
		groups[gid] = server
	}
	dist := getBalancedDistribution(NShards, len(groups))
	gsMap := getGidShardMap(conf.Shards)
	gidByCount := getGidSortedByCountArray(conf.Groups, conf.Shards)
	freeShard := make([]int, 0)

	if conf.Shards[0] == 0 {
		gidByCount = make([]int, 0)
		gsMap = make(map[int][]int)
		for i := 0; i < NShards; i++ {
			freeShard = append(freeShard, i)
		}
	}

	for gid := range servers {
		gsMap[gid] = make([]int, 0)
		gidByCount = append(gidByCount, gid)
	}

	for i, gid := range gidByCount {
		for len(gsMap[gid]) > dist[i] {
			freeShard = append(freeShard, gsMap[gid][0])
			gsMap[gid] = gsMap[gid][1:]
		}
		for len(gsMap[gid]) < dist[i] {
			gsMap[gid] = append(gsMap[gid], freeShard[0])
			freeShard = freeShard[1:]
		}
	}

	shards := gidShardMapToShards(gsMap)
	result := Config{
		Num:    conf.Num + 1,
		Shards: shards,
		Groups: groups,
	}
	return result
}
func leave(conf Config, gids []int) Config {
	groups := copyOfGroups(conf.Groups)
	for _, gid := range gids {
		_, prs := groups[gid]
		assert(prs)
		delete(groups, gid)
	}

	if len(groups) == 0 {
		shards := [NShards]int{}
		return Config{
			Num:    conf.Num + 1,
			Shards: shards,
			Groups: groups,
		}
	}
	dist := getBalancedDistribution(NShards, len(groups))
	gsMap := getGidShardMap(conf.Shards)
	gidByCount := getGidSortedByCountArray(conf.Groups, conf.Shards)

	freeShard := make([]int, 0)
	for _, gid := range gids {
		gidByCount = deleteSlice(gidByCount, gid)
		freeShard = append(freeShard, gsMap[gid]...)
		delete(gsMap, gid)
	}
	for i, gid := range gidByCount {
		for len(gsMap[gid]) > dist[i] {
			freeShard = append(freeShard, gsMap[gid][0])
			gsMap[gid] = gsMap[gid][1:]
		}
		for len(gsMap[gid]) < dist[i] {
			gsMap[gid] = append(gsMap[gid], freeShard[0])
			freeShard = freeShard[1:]
		}
	}

	shards := gidShardMapToShards(gsMap)
	result := Config{
		Num:    conf.Num + 1,
		Shards: shards,
		Groups: groups,
	}
	return result
}
func move(conf Config, shard int, gid int) Config {
	// arrays should be copied by value
	shards := conf.Shards
	shards[shard] = gid
	groups := copyOfGroups(conf.Groups)
	result := Config{
		Num:    conf.Num + 1,
		Shards: shards,
		Groups: groups,
	}
	return result
}

func deleteSlice(s []int, a int) []int {
	for i, v := range s {
		if v == a {
			return append(s[:i], s[i+1:]...)
		}
	}
	panic("element not exist in slice!")
}

func copyOfGroups(m map[int][]string) map[int][]string {
	result := make(map[int][]string)
	for k, v := range m {
		result[k] = v
	}
	return result
}

func gidShardMapToShards(gsMap map[int][]int) [NShards]int {
	// initialialized to 0s, so if no group in gsMap, will return invalid(correct) result
	var result [NShards]int
	for gid, shards := range gsMap {
		for _, shard := range shards {
			result[shard] = gid
		}
	}
	return result
}

func getGidShardMap(shards [NShards]int) map[int][]int {
	gidMap := make(map[int][]int)
	for shard, gid := range shards {
		if _, prs := gidMap[gid]; prs {
			gidMap[gid] = append(gidMap[gid], shard)
		} else {
			gidMap[gid] = make([]int, 1)
			gidMap[gid][0] = shard
		}
	}

	return gidMap
}

func getGidSortedByCountArray(groups map[int][]string, shards [NShards]int) []int {
	// fix: include those that aren't assigned any shard
	gidCount := make(map[int]int)
	for g := range groups {
		gidCount[g] = 0
	}
	for _, gid := range shards {
		gidCount[gid]++
	}

	slice := make([]int, 0, len(gidCount))
	for gid := range gidCount {
		slice = append(slice, gid)
	}
	sort.Slice(slice, func(i, j int) bool {
		return gidCount[slice[i]] > gidCount[slice[j]]
	})
	return slice
}

func getBalancedDistribution(n int, p int) []int {
	a := n / p
	r := n - p*a
	result := make([]int, p)
	for i := 0; i < p; i++ {
		if r != 0 {
			result[i] = a + 1
			r--
		} else {
			result[i] = a
		}
	}
	return result
}

func assert(cond bool) {
	if !cond {
		panic("assert failed!")
	}
}
