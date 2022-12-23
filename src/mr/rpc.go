package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

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

type MapArgs struct {
	FinishedNum int // 完成的任务编号
}

//type MapReply struct {
//	HasTask bool // 是否还有任务
//	FileName int // Map的文件名
//}
type InitReply struct {
	NReduce     int
	MapTaskSize int
}
type FinishedReply struct {
	MapFinished    bool
	ReduceFinished bool
}
type MapReply struct {
	FileName string // map的文件名
	TaskNum  int    // 任务编号
}
type MapTaskAssign struct {
	FileName string // map的文件名
	TaskNum  int    // 任务编号
	Assigned bool   // 是否已被分配
	Finished bool   // 是否已完成
}

type ReduceTaskAssign struct {
	TaskNum  int  // 任务编号
	Assigned bool // 是否已被分配
	Finished bool // 是否已完成
}
type ReduceArgs struct {
	FinishedNum int
}

type ReduceReply struct {
	ReduceNum int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
