package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	// Your definitions here.
	mu          sync.Mutex
	done        bool
	nReduce     int
	nMap        int
	files       []string
	mapTasks    []int //positive for id, 0 for waiting, negative for finish
	reduceTasks []int //same as mapTasks
	idleCounter int
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

func (c *Coordinator) Finish(args *TaskDoneArgs, reply *TaskDoneReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if args.TaskType == 0 {
		c.mapTasks[args.Index] = -1
		if DEBUG {
			fmt.Printf("Map task %v finished\n", args.Index)
		}
		// return nil
	}
	if args.TaskType == 1 {
		c.reduceTasks[args.Index] = -1
		if DEBUG {
			fmt.Printf("Reduce task %v finished\n", args.Index)
		}
		// return nil
	}
	for _, state := range c.mapTasks {
		if state >= 0 {
			return nil
		}
	}
	for _, state := range c.reduceTasks {
		if state >= 0 {
			return nil
		}
	}
	c.done = true
	return nil
}

func (c *Coordinator) RequestTask(args *RequestTask, reply *RequestTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.idleCounter > 10 {
		//someone is crashed
		for i := 0; i < c.nMap; i++ {
			if c.mapTasks[i] == 0 {
				c.mapTasks[i] = 1
			}
		}
		for i := 0; i < c.nReduce; i++ {
			if c.reduceTasks[i] == 0 {
				c.reduceTasks[i] = 1
			}
		}
	}
	mapFinish := true
	for i := 0; i < c.nMap; i++ {
		state := c.mapTasks[i]
		if state > 0 {
			reply.TaskType = 0
			reply.Index = i
			reply.MapFileName = c.files[i]
			reply.NReduce = c.nReduce
			c.mapTasks[i] = 0
			if DEBUG {
				fmt.Printf("Send map task %v\n", reply)
			}
			c.idleCounter = 0
			return nil
		}
		if state >= 0 {
			mapFinish = false
		}
	}
	if mapFinish {
		for i := 0; i < c.nReduce; i++ {
			state := c.reduceTasks[i]
			if state > 0 {
				reply.TaskType = 1
				reply.Index = i
				reply.NReduce = c.nReduce
				reply.NMap = c.nMap
				c.reduceTasks[i] = 0
				if DEBUG {
					fmt.Printf("Send reduce task %v\n", reply)
				}
				c.idleCounter = 0
				return nil
			}
		}
	}
	reply.TaskType = -1 //worker stay idle
	c.idleCounter++
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
	c.mu.Lock()
	defer c.mu.Unlock()
	res := c.done
	return res
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.files = files
	c.mapTasks = make([]int, len(files))
	c.reduceTasks = make([]int, nReduce)
	c.nReduce = nReduce
	c.nMap = len(files)
	for i := 0; i < c.nMap; i++ {
		c.mapTasks[i] = 1
	}
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = 1
	}
	c.mu = sync.Mutex{}
	// Your code here.

	c.server()
	return &c
}
