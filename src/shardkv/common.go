package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK              = "OK"
	ErrNoKey        = "ErrNoKey"
	ErrWrongGroup   = "ErrWrongGroup"
	ErrLeaderChange = "ErrLeaderChange"
	ErrShardNotReady = "ErrShardNotReady"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientID int64
	SeqNum   int
	ConfigNum int
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientID int64
	SeqNum   int
	ConfigNum int
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}

type RequestShardArgs struct {
	ShardNum  int
	ConfigNum int
}

type RequestShardReply struct {
	Success bool
	Data    Shard
}

type GetConfigNumArgs struct {
}

type GetConfigNumReply struct {
	Num int
}
