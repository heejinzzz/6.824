package mr

import (
	"encoding/gob"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const defaultExecuteTimeout = 10 * time.Second

type Coordinator struct {
	// Your definitions here.
	nMap            int
	nReduce         int
	idleTasks       map[int]Task
	onProgressTasks map[int]Task
	mutex           sync.Mutex
	executeTimeout  time.Duration
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) RequestTask(_ *RequestTaskArgs, reply *RequestTaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if len(c.idleTasks) == 0 && len(c.onProgressTasks) == 0 {
		return ErrNoMoreTask
	}
	if len(c.idleTasks) == 0 {
		return ErrNoIdleTask
	}
	for i, task := range c.idleTasks {
		reply.Task = task
		c.onProgressTasks[task.Id()] = task
		delete(c.idleTasks, i)
		go c.waitForExecute(task)
		break
	}
	return nil
}

func (c *Coordinator) waitForExecute(waitingTask Task) {
	time.Sleep(c.executeTimeout)
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if task, ok := c.onProgressTasks[waitingTask.Id()]; ok && task.Type() == waitingTask.Type() {
		//log.Printf("Task: %v Execute Timeout", waitingTask)
		//log.Printf("Reset Task: %v", waitingTask)
		delete(c.onProgressTasks, waitingTask.Id())
		c.idleTasks[waitingTask.Id()] = waitingTask
	}
}

func (c *Coordinator) InformDone(args *InformDoneArgs, _ *InformDoneReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if task, ok := c.idleTasks[args.Task.Id()]; ok {
		if task.Type() == args.Task.Type() {
			delete(c.idleTasks, task.Id())
			if len(c.idleTasks) == 0 && len(c.onProgressTasks) == 0 && args.Task.Type() == TaskTypeMap {
				for i := 0; i < c.nReduce; i++ {
					c.idleTasks[i] = NewReduceTask(i, c.nMap)
				}
			}
		}
		return nil
	}
	if task, ok := c.onProgressTasks[args.Task.Id()]; ok {
		if task.Type() == args.Task.Type() {
			delete(c.onProgressTasks, task.Id())
			if len(c.idleTasks) == 0 && len(c.onProgressTasks) == 0 && args.Task.Type() == TaskTypeMap {
				for i := 0; i < c.nReduce; i++ {
					c.idleTasks[i] = NewReduceTask(i, c.nMap)
				}
			}
		}
	}
	return nil
}

func (c *Coordinator) InformError(args *InformErrorArgs, _ *InformErrorReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if task, ok := c.onProgressTasks[args.Task.Id()]; ok && task.Type() == args.Task.Type() {
		//log.Printf("Worker Executing Error: %s", args.ErrorMessage)
		//log.Printf("Reset Task: %v", args.Task)
		delete(c.onProgressTasks, task.Id())
		c.idleTasks[args.Task.Id()] = args.Task
	}
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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
	c.mutex.Lock()
	defer c.mutex.Unlock()

	return len(c.idleTasks) == 0 && len(c.onProgressTasks) == 0
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	gob.Register(&MapTask{})
	gob.Register(&ReduceTask{})

	idleTasks := map[int]Task{}
	for i, file := range files {
		idleTasks[i] = NewMapTask(i, file, nReduce)
	}

	c := Coordinator{
		nMap:            len(files),
		nReduce:         nReduce,
		idleTasks:       idleTasks,
		onProgressTasks: map[int]Task{},
		executeTimeout:  defaultExecuteTimeout,
	}

	// Your code here.

	c.server()
	return &c
}
