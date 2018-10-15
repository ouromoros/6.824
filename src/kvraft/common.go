package raftkv

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Identifier int64
	PrevIndex	 int
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
	Index				int
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Identifier int64
	PrevIndex		int
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
	Index				int
}
