package mr

import (
	"container/list"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

var (
	MapFileName   = "" // map文件名
	MapTaskNum    = -1 // map任务号
	MapTaskSize   = -1 // map总任务数
	ReduceTaskNum = -1 // reduce任务号
)

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	getNReduce()
	// 连接master,获取map任务

	for {
		getMapTask(MapTaskNum)
		if MapTaskNum == MapFinished {
			break
		} else {
			// 如果无任务
			if MapTaskNum == NoTask && MapFileName == "" {
				time.Sleep(time.Second)
				continue
			}
			executeMap(MapFileName, mapf)
		}

	}

	// 等待执行Reduce任务
	for {
		getReduceTask(ReduceTaskNum)
		if ReduceTaskNum == ReduceFinished {
			return
		}
		// 如果没有完成map，等待1s再重试
		if ReduceTaskNum == WaitMap {
			time.Sleep(time.Second)
			continue
		}
		// 如果没有任务，等待1s
		if ReduceTaskNum == NoTask {
			time.Sleep(time.Second)
			continue
		}
		executeReduce(reducef)
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func executeMap(fileName string, mapf func(string, string) []KeyValue) {
	var intermediate []KeyValue
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	file.Close()
	kva := mapf(fileName, string(content))
	intermediate = append(intermediate, kva...)

	sort.Sort(ByKey(intermediate))
	var reduceHash []*list.List

	// 初始化list
	for i := 0; i < NReduce; i++ {
		reduceHash = append(reduceHash, list.New())
	}
	for i := 0; i < len(intermediate); i++ {
		// 对每个key都计算Reduce号
		keyValue := intermediate[i]
		reduceNum := ihash(keyValue.Key) % NReduce
		reduceHash[reduceNum].PushBack(intermediate[i])
	}

	// 依次遍历每个列表（Reduce桶）
	for i := 0; i < len(reduceHash); i++ {
		// 写入json文件
		//jsonFileName := "./intermediate/mr-" + strconv.Itoa(MapTaskNum) + "-" + strconv.Itoa(i) + ".json"
		jsonFileName := "mr-" + strconv.Itoa(MapTaskNum) + "-" + strconv.Itoa(i) + ".json"
		filePtr, _ := os.Create(jsonFileName)
		encoder := json.NewEncoder(filePtr)
		reduceList := reduceHash[i]
		// 遍历这个reduce号的所有键值对，写入json文件
		for i := reduceList.Front(); i != nil; i = i.Next() {
			encoder.Encode(i.Value)
		}
	}
}

func executeReduce(reducef func(string, []string) string) {
	var kva []KeyValue
	// 读取每个中间文件
	for i := 0; i < MapTaskSize; i++ {
		//fileName := "/home/oslab/6.824/src/main/intermediate/mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(ReduceTaskNum) + ".json"
		fileName := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(ReduceTaskNum) + ".json"
		file, _ := os.Open(fileName)
		//fmt.Println(fileName)
		//content, _ := ioutil.ReadAll(file)
		//fmt.Println(content)
		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				if err == io.EOF {
					break
				} else {
					log.Fatalf("%v", err)
				}
			}
			kva = append(kva, kv)
		}
		file.Close()

		//outFileName := "./out/mr-out-" + strconv.Itoa(ReduceTaskNum) + ".txt"

	}
	sort.Sort(ByKey(kva))
	outFileName := "mr-out-" + strconv.Itoa(ReduceTaskNum) + ".txt"
	outFile, _ := os.Create(outFileName)

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(outFile, "%v %v\n", kva[i].Key, output)

		i = j
	}
	outFile.Close()

}
func getMapTask(n int) {
	args := MapArgs{n}

	reply := MapReply{}

	ok := call("Coordinator.AssignMapTask", &args, &reply)
	if ok {
		MapTaskNum = reply.TaskNum
		MapFileName = reply.FileName
		fmt.Println("got map task:", reply.TaskNum, " ", reply.FileName)
	} else {
		fmt.Printf("call failed!\n")
	}
}
func getReduceTask(n int) {
	args := ReduceArgs{n}

	reply := ReduceReply{}

	ok := call("Coordinator.AssignReduceTask", &args, &reply)
	if ok {
		ReduceTaskNum = reply.ReduceNum
		if ReduceTaskNum == WaitMap {
			fmt.Println("waiting for map to finish")
		} else if ReduceTaskNum == NoTask {
			fmt.Println("no more reduce task")
		} else {
			//fmt.Println("got reduce task:", reply.ReduceNum)
		}
	} else {
		fmt.Printf("call failed!\n")

	}
}
func getNReduce() {
	args := MapArgs{0}

	reply := InitReply{}
	ok := call("Coordinator.GetNReduce", &args, &reply)
	if ok {
		NReduce = reply.NReduce
		MapTaskSize = reply.MapTaskSize
		//fmt.Println("got NReduce:", NReduce, "size:", MapTaskSize)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
