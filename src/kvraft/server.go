package kvraft

import (
	"course/labgob"
	"course/labrpc"
	"course/raft"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	dead         int32 // set by Kill()
	maxraftstate int   // snapshot if log grows this big

	// Your definitions here.
	lastApplied  int
	stateMachine *MemoryKVStateMachine
	notifyChans  map[int]chan *OpReply
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	index, _, isLeader := kv.rf.Start(Op{Key: args.Key, OpType: OpGet})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// wait for result
	kv.mu.Lock()
	notifyCh := kv.getNotifyChannel(index)
	kv.mu.Unlock()

	select {
	case result := <-notifyCh:
		reply.Value = result.Value
		reply.Err = result.Err
	case <-time.After(ClientRequestTimeOut):
		reply.Err = ErrTimeout
	}
	// async way to delete channel
	go func() {
		kv.mu.Lock()
		kv.removeNotifyChannel(index)
		kv.mu.Unlock()
	}()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	index, _, isLeader := kv.rf.Start(
		Op{
			Key:    args.Key,
			Value:  args.Value,
			OpType: getOperationType(args.Op),
		},
	)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// wait for result
	kv.mu.Lock()
	notifyCh := kv.getNotifyChannel(index)
	kv.mu.Unlock()

	select {
	case result := <-notifyCh:
		reply.Err = result.Err
	case <-time.After(ClientRequestTimeOut):
		reply.Err = ErrTimeout
	}

	// async way to delete channel
	go func() {
		kv.mu.Lock()
		kv.removeNotifyChannel(index)
		kv.mu.Unlock()
	}()
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

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
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.lastApplied = 0
	kv.dead = 0
	kv.stateMachine = NewMachine()
	kv.notifyChans = make(map[int]chan *OpReply)
	go kv.applyTask()
	return kv
}

func (kv *KVServer) applyTask() {
	for !kv.killed() {
		select {
		case message := <-kv.applyCh:
			if message.CommandValid {
				kv.mu.Lock()
				// check whether applied before
				if message.CommandIndex <= kv.lastApplied {
					kv.mu.Unlock()
					continue
				}
				op := message.Command.(Op)
				opReply := kv.applyToStateMachine(op)

				// return result
				if _, isLeader := kv.rf.GetState(); isLeader {
					notifyCh := kv.getNotifyChannel(message.CommandIndex)
					notifyCh <- opReply
				}
				kv.mu.Unlock()
			}
		}
	}
}

func (kv *KVServer) applyToStateMachine(op Op) *OpReply {
	var value string
	var err Err
	switch op.OpType {
	case OpGet:
		value, err = kv.stateMachine.Get(op.Key)
	case OpPut:
		err = kv.stateMachine.Put(op.Key, op.Value)
	case OpAppend:
		err = kv.stateMachine.Append(op.Key, op.Value)
	default:
		panic(fmt.Sprintf("unknow oPType:%d", op.OpType))
	}
	return &OpReply{
		Value: value,
		Err:   err,
	}
}

func (kv *KVServer) getNotifyChannel(index int) chan *OpReply {
	if _, ok := kv.notifyChans[index]; !ok {
		kv.notifyChans[index] = make(chan *OpReply)
	}
	return kv.notifyChans[index]
}
func (kv *KVServer) removeNotifyChannel(index int) {
	if _, ok := kv.notifyChans[index]; ok {
		delete(kv.notifyChans, index)
	}
}
