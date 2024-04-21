package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type TaskArgs struct {
	WIndex int32
}

var (
	TaskType_Map    int32 = 0
	TaskType_Reduce int32 = 1

	Task_Status_OK    int32 = 200
	Task_Status_ERROR int32 = 500
)

type TaskReply struct {
	TaskType  int32
	FileName  string
	TaskIndex int
	Reduce    int32
	NumFiles  int
}

type TaskStatusArg struct {
	TaskType  int32
	FileName  string
	Reduce    int32
	TaskIndex int
	Status    int32
}

type TaskStatus struct {
	TaskType int32
	FileName string
	Reduce   int32
	Status   int32
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
