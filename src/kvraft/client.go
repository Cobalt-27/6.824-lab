package kvraft

import (
	"crypto/rand"
	// "fmt"
	"math/big"
	// "runtime"
	"time"

	"6.824/labrpc"
)

const (
	opPut    = 0
	opAppend = 1
	opGet    = 2
	noop     = 3
)

const pollInterval int = 100

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
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
	// fmt.Printf("gonum=%v\n", runtime.NumGoroutine())
	// You'll have to add code here.
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
	DPrintf("###GET[%v]\n", key)
	for {
		for i, remote := range ck.servers {
			args := GetArgs{
				Key: key,
				Uid: genUid(),
			}
			reply := GetReply{}
			DPrintf("RPC GET[%v]", key)
			ok := remote.Call("KVServer.Get", &args, &reply)
			DPrintf("RPC END GET[%v] err=%v ok=%v", key, reply.Err, ok)
			if !ok {
				DPrintf("sendGet to [%v] failed\n", i)
				continue
			} else {
				DPrintf("<< GET[%v] %v,%v\n", key, reply.Value, reply.Err)
				if reply.Err == "OK" {
					return reply.Value
				}
			}
		}
		time.Sleep(time.Duration(pollInterval * int(time.Millisecond)))
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
	// You will have to modify this function.
	DPrintf("###%v[%v:%v]\n", op, key, value)
	myUid := genUid()
	for {
		for i, remote := range ck.servers {
			args := PutAppendArgs{
				Key:   key,
				Value: value,
				Op:    op,
				Uid:   myUid,
			}
			reply := PutAppendReply{}
			DPrintf("RPC %v %v:%v", op, key, value)
			ok := remote.Call("KVServer.PutAppend", &args, &reply)
			DPrintf("RPC END %v %v:%v", op, key, value)
			if !ok {
				DPrintf("%v to [%v] failed\n", op, i)
				// return
				continue
			} else {
				DPrintf("<< %v[%v:%v] %v\n", op, key, value, reply.Err)
				if reply.Err == "OK" {
					return
				}
			}
		}
		time.Sleep(time.Duration(pollInterval * int(time.Millisecond)))
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
