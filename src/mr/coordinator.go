package mr

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"
import _ "net/http/pprof"

var (
	mapTasks          []MapTaskAssign    // 所有的map任务
	mapTimers         []*time.Timer      // 所有的map任务
	reduceTimers      []*time.Timer      // 所有的map任务
	reduceTasks       []ReduceTaskAssign // 所有的map任务
	NoTask            = -1               // 暂时无任务
	MapFinished       = -10              // 无任务
	ReduceFinished    = -20              // 无任务
	WaitMap           = -2               // 需要等待map全部完成
	NReduce           = 0
	FinishedReduceNum = int32(0) // 已完成reduce数
	FinishedMapNum    = int32(0) // 已完成reduce数
	mapMutex          = sync.RWMutex{}
	reduceMutex       = sync.RWMutex{}
)

type Coordinator struct {
	// Your definitions here.

}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// AssignMapTask 分配map任务
func (c *Coordinator) AssignMapTask(args *MapArgs, reply *MapReply) error {

	// 如果全部都执行完了
	if judgeMapAllFinished() {
		fmt.Println("all map task has finished!")
		reply.TaskNum = MapFinished
		return nil
	}
	// 如果完成map任务，则更新状态
	if args.FinishedNum != NoTask {
		//fmt.Println("update finish map task")
		mapMutex.Lock()
		mapTasks[args.FinishedNum].Finished = true
		atomic.AddInt32(&FinishedMapNum, 1)
		mapTimers[args.FinishedNum].Stop()
		fmt.Println("map:", args.FinishedNum, "finished, timer stoped")
		mapMutex.Unlock()
		//fmt.Println("update map task finished")
	}

	// 是否还有任务
	moreTask := false
	//fmt.Println(mapTasks)
	// 分配map任务
	mapMutex.Lock()
	for i := range mapTasks {
		if !mapTasks[i].Assigned && !mapTasks[i].Finished {
			//fmt.Println("assigning map task")
			mapTasks[i].Assigned = true
			reply.FileName = mapTasks[i].FileName
			reply.TaskNum = mapTasks[i].TaskNum
			timer := time.AfterFunc(10*time.Second, func() {
				mapTasks[i].Assigned = false
				mapTasks[i].Finished = false
				fmt.Println("map:", i, "crashed, reassign map")

			})
			mapTimers = append(mapTimers, timer)
			moreTask = true
			//fmt.Println("assigning map task finished")
			break
		}
	}
	mapMutex.Unlock()

	// 任务分配完了
	if !moreTask {
		fmt.Println("all map task has been assigned!")
		reply.TaskNum = NoTask
	}
	//log.Println("mapTask ", reply.TaskNum, " Assigned!")
	return nil
}

// AssignReduceTask 分配Reduce任务
func (c *Coordinator) AssignReduceTask(args *ReduceArgs, reply *ReduceReply) error {

	// 如果全部都执行完了
	if judgeReduceAllFinished() {
		fmt.Println("all reduce task has finished!")
		reply.ReduceNum = ReduceFinished
		return nil
	}
	// 判断map任务是否全都已完成
	if !judgeMapAllFinished() {
		reply.ReduceNum = WaitMap
		return nil
	}

	// 如果完成reduce任务，则更新状态
	if args.FinishedNum != NoTask && args.FinishedNum != WaitMap && args.FinishedNum != ReduceFinished {
		//fmt.Println("update finish reduce task")
		reduceMutex.Lock()
		reduceTasks[args.FinishedNum].Finished = true
		atomic.AddInt32(&FinishedReduceNum, 1)
		reduceMutex.Unlock()
		//fmt.Println("update reduce task finished")
	}

	// 是否还有任务
	moreTask := false
	//fmt.Println(reduceTasks)
	// 分配reduce任务
	reduceMutex.Lock()
	for i := range reduceTasks {
		if !reduceTasks[i].Assigned && !reduceTasks[i].Finished {
			//fmt.Println("assigning reduce task")
			reduceTasks[i].Assigned = true
			timer := time.AfterFunc(10*time.Second, func() {
				fmt.Println("reduce:", i, "crashed, reassign reduce")
				reduceTasks[i].Assigned = false
			})
			reduceTimers = append(reduceTimers, timer)
			//fmt.Println("assigning reduce task finished")
			reply.ReduceNum = reduceTasks[i].TaskNum
			moreTask = true
			break
		}
	}
	reduceMutex.Unlock()

	// 任务分配完了
	if !moreTask {
		reply.ReduceNum = NoTask
	}
	//log.Println("reduceTask ", reply.ReduceNum, " Assigned!")
	return nil
}

func judgeMapAllFinished() bool {

	return int(atomic.LoadInt32(&FinishedMapNum)) == len(mapTasks)
}

func judgeReduceAllFinished() bool {
	return int(atomic.LoadInt32(&FinishedReduceNum)) == len(reduceTasks)

}

func (c *Coordinator) GetNReduce(args *MapArgs, reply *InitReply) error {
	reply.NReduce = NReduce
	reply.MapTaskSize = len(mapTasks)
	return nil
}

// start a thread that listens for RPCs from worker.go

func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// Your code here.
	if judgeReduceAllFinished() {
		fmt.Println("mapreduce finished!exiting")
		return true
	}
	return false
}

func readAllInputFile(files []string) {
	i := 0
	for _, fileName := range files {
		task := MapTaskAssign{
			FileName: fileName,
			TaskNum:  i,
			Assigned: false,
			Finished: false,
		}
		mapTasks = append(mapTasks, task)
		i++
	}
	for i := 0; i < NReduce; i++ {
		reduce := ReduceTaskAssign{
			TaskNum:  i,
			Assigned: false,
			Finished: false,
		}
		reduceTasks = append(reduceTasks, reduce)
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	NReduce = nReduce

	readAllInputFile(files)
	//fmt.Println("nReduce:", NReduce)
	c.server()
	return &c
}
