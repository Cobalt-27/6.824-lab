package kvraft

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func genUid() string {
	return fmt.Sprintf("%v%v%v", nrand(), nrand(), nrand())
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType int
	Uid    string
	OpKey  string
	OpVal  string
}

type KVServer struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	dead         int32 // set by Kill()
	maxraftstate int   // snapshot if log grows this big

	// Your definitions here.
	state    map[string]string
	uidUsed  map[string]bool
	doneChan map[int]chan string

	index int
}

func (kv *KVServer) print(format string, a ...interface{}) {
	head := fmt.Sprintf("KV[%v]", kv.me)
	DPrintf(head+format, a...)
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.print("SERVER GET")
	kv.mu.Lock()
	unlock := true
	defer func() {
		if unlock {
			kv.mu.Unlock()
		}
	}()
	myUid := args.Uid
	if _, ok := kv.state[args.Key]; !ok {
		reply.Err = ErrNoKey
	}
	index, _, isLeader := kv.rf.Start(Op{
		OpType: opGet,
		OpKey:  args.Key,
		OpVal:  "",
		Uid:    myUid,
	})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	if kv.doneChan[index] != nil {
		kv.print("chan send")
		kv.doneChan[index] <- ""
		kv.print("chan send end")
	}
	done := make(chan string, 1)
	kv.doneChan[index] = done
	kv.mu.Unlock()
	var uid string
	if args.Timeout {
		select {
		case uid = <-done:
		case <-time.After(time.Duration(pollInterval * 10 * int(time.Millisecond))):
			reply.Err = "Timeout"
			unlock = false
			return
		}
	} else {
		kv.print("unlock and wait for %v", kv.index)
		uid = <-done
		kv.print("done")
	}
	kv.mu.Lock()
	kv.print("locked")
	if uid == myUid {
		reply.Err = OK
	} else {
		reply.Err = "Wrong UID"
	}
	reply.Value = kv.state[args.Key]
	// Your code here.
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	myUid := args.Uid
	if args.Op == "Put" || args.Op == "Append" {
		var opType int
		if args.Op == "Put" {
			opType = opPut
		} else {
			opType = opAppend
		}
		index, _, isLeader := kv.rf.Start(Op{
			OpType: opType,
			OpKey:  args.Key,
			OpVal:  args.Value,
			Uid:    myUid,
		})
		if !isLeader {
			reply.Err = ErrWrongLeader
			return
		}
		if kv.doneChan[index] != nil {
			kv.print("chan send")
			kv.doneChan[index] <- ""
			kv.print("chan send end")
		}
		done := make(chan string, 1)
		kv.doneChan[index] = done
		kv.mu.Unlock()
		kv.print("unlock and wait for %v", index)
		uid := <-done
		kv.print("done")
		kv.mu.Lock()
		kv.print("locked")
		if uid == myUid {
			reply.Err = OK
		} else {
			reply.Err = "Wrong UID"
		}
		return
	} else {
		panic("invalid op")
	}

	// Your code here.
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) takeSnapshot() {
	for !kv.killed() {
		kv.mu.Lock()
		// if kv.rf.GetState()
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(kv.state)
		e.Encode(kv.uidUsed)
		snapshot := w.Bytes()
		kv.rf.Snapshot(kv.index, snapshot)
		kv.mu.Unlock()
	}

}

func (kv *KVServer) installSnapshot(snapshot []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	if d.Decode(kv.state) != nil || d.Decode(kv.uidUsed) != nil {
		panic("snapshot install error")
	}
}

func (kv *KVServer) fillNoop() {
	for !kv.killed() {
		args := GetArgs{
			Key:     "noop",
			Uid:     genUid(),
			Timeout: true,
		}
		reply := GetReply{}
		go kv.Get(&args, &reply)
		// DPrintf("SERVER FILL")
		time.Sleep(time.Duration(pollInterval * int(time.Millisecond) * 50))
	}
}

func (kv *KVServer) listen() {
	const listenTimeout = 300
	for !kv.killed() {
		var apply raft.ApplyMsg
		select {
		case <-time.After(listenTimeout * time.Millisecond):
			continue
		case apply = <-kv.applyCh:
			if (apply.CommandValid && apply.SnapshotValid) || (!apply.CommandValid && !apply.SnapshotValid) {
				panic("invalid apply msg")
			}
		}
		kv.mu.Lock()
		if apply.CommandValid {
			op := apply.Command.(Op)
			kv.print("APPLY %v %v:%v", op.OpType, op.OpKey, op.OpVal)
			kv.print("UID=%v", op.Uid)
			used := false
			if kv.uidUsed[op.Uid] {
				kv.print("UID USED")
				used = true
			}
			kv.uidUsed[op.Uid] = true
			if !used {
				if op.OpType == opPut {
					kv.state[op.OpKey] = op.OpVal
				} else if op.OpType == opAppend {
					kv.state[op.OpKey] += op.OpVal
				}
			}
			kv.index++
			if kv.index != apply.CommandIndex {
				panic(fmt.Sprintf("applied %v!=kv idx %v", apply.CommandIndex, kv.index))
			}
			kv.print("Apply %v", kv.index)
			if kv.doneChan[kv.index] != nil {
				kv.doneChan[kv.index] <- op.Uid
				kv.print("Apply sent %v", kv.index)
				delete(kv.doneChan, kv.index)
			}
		} else if apply.SnapshotValid {
			kv.index = apply.SnapshotIndex
			kv.installSnapshot(apply.Snapshot)
		}
		kv.mu.Unlock()
		kv.print("APPLY END")
	}
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
	kv.index = 0
	kv.state = map[string]string{}
	kv.uidUsed = map[string]bool{}
	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.doneChan = make(map[int]chan string)
	// You may need initialization code here.
	go kv.listen()
	go kv.fillNoop()
	return kv
}
