package shardmaster

//
// Shardmaster clerk.
//

import "labrpc"
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
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
	// Your code here.
	ck.prevLeader = 0
	ck.nextSeqNum = 0
	ck.id = nrand()
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	args.ClientID = ck.id
	args.SeqNum = ck.nextSeqNum

	numServer := len(ck.servers)
	for {
		// try each known server.
		for i := 0; ; i = (i + 1) % numServer {
			var reply QueryReply
			server := (i + ck.prevLeader) % numServer
			ok := ck.servers[server].Call("ShardMaster.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.prevLeader = server
				ck.nextSeqNum++
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	args.ClientID = ck.id
	args.SeqNum = ck.nextSeqNum

	numServer := len(ck.servers)

	for {
		// try each known server.
		for i := 0; ; i = (i + 1) % numServer {
			var reply QueryReply
			server := (i + ck.prevLeader) % numServer
			ok := ck.servers[server].Call("ShardMaster.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.prevLeader = server
				ck.nextSeqNum++
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}


func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	args.ClientID = ck.id
	args.SeqNum = ck.nextSeqNum

	numServer := len(ck.servers)

	for {
		// try each known server.
		for i := 0; ; i = (i + 1) % numServer {
			var reply LeaveReply
			server := (i + ck.prevLeader) % numServer
			ok := ck.servers[server].Call("ShardMaster.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.prevLeader = server
				ck.nextSeqNum++
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	args.ClientID = ck.id
	args.SeqNum = ck.nextSeqNum

	numServer := len(ck.servers)

	for {
		// try each known server.
		for i := 0; ; i = (i + 1) % numServer {
			var reply MoveReply
			server := (i + ck.prevLeader) % numServer
			ok := ck.servers[server].Call("ShardMaster.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.prevLeader = server
				ck.nextSeqNum++
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
