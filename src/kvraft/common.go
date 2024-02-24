package kvraft

import (
	"fmt"
	"time"
)

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
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
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

const ClientRequestTimeOut = time.Duration(time.Millisecond * 500)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key    string
	Value  string
	OpType OperationType
}

type OperationType uint8

const (
	OpGet OperationType = iota
	OpPut
	OpAppend
)

// OpReply return of applyToStateMachine
type OpReply struct {
	Value string
	Err   Err
}

func getOperationType(v string) OperationType {
	switch v {
	case "Get":
		return OpGet
	case "Put":
		return OpPut
	case "Append":
		return OpAppend
	default:
		panic(fmt.Sprintf("unknow operation type :%s", v))
	}
}
