package mr

import (
	"encoding/gob"
	"fmt"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type KVs []KeyValue

func (kvs KVs) Len() int {
	return len(kvs)
}

func (kvs KVs) Less(i, j int) bool {
	return kvs[i].Key < kvs[j].Key
}

func (kvs KVs) Swap(i, j int) {
	kvs[i], kvs[j] = kvs[j], kvs[i]
}

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

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	gob.Register(&MapTask{})
	gob.Register(&ReduceTask{})

	for {
		task, err := CallRequestTask()
		if err != nil && err.Error() == ErrNoIdleTask.Error() {
			time.Sleep(time.Second)
			continue
		}
		if err != nil {
			break
		}
		if mapTask, ok := task.(*MapTask); ok {
			err = mapTask.Execute(mapf)
			if err != nil {
				err = CallInformError(mapTask, err)
				if err != nil {
					break
				}
			} else {
				err = CallInformDone(mapTask)
				if err != nil {
					break
				}
			}
		} else {
			reduceTask := task.(*ReduceTask)
			err = reduceTask.Execute(reducef)
			if err != nil {
				err = CallInformError(reduceTask, err)
				if err != nil {
					break
				}
			} else {
				err = CallInformDone(reduceTask)
				if err != nil {
					break
				}
			}
		}
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	err := call("Coordinator.Example", &args, &reply)
	if err == nil {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func CallRequestTask() (Task, error) {
	args := RequestTaskArgs{}
	reply := RequestTaskReply{}
	err := call("Coordinator.RequestTask", &args, &reply)
	if err != nil {
		return nil, err
	}
	return reply.Task, nil
}

func CallInformDone(task Task) error {
	args := InformDoneArgs{Task: task}
	reply := InformDoneReply{}
	return call("Coordinator.InformDone", &args, &reply)
}

func CallInformError(task Task, err error) error {
	args := InformErrorArgs{Task: task, ErrorMessage: err.Error()}
	reply := InformErrorReply{}
	return call("Coordinator.InformError", &args, &reply)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns nil.
// returns error if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) error {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	return c.Call(rpcname, args, reply)
}
