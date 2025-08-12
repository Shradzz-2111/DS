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
	mapTaskId         map[int]string   // Number of map tasks
	nReducers         int              // number of reduce tasks
	MapStatus         map[string]int   // status of file
	reduceStatus      map[int]int      // status of reduce tasks
	intermediateFiles map[int][]string // intermediate files for each reduce task
	mu                sync.Mutex       // Mutex to protect shared state
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) WorkerAssignment(args *WorkerTaskRequestArgs, reply *WorkerTaskRequestReply) error {
	// Assign a worker ID or task here.
	reply.WorkerId = args.WorkerId // Just echoing back the ID for now.
	reply.TaskName = "MAP"         // Example task name
	reply.Done = false             // Indicating the task is not done
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	for _, v := range c.reduceStatus {
		if v != 1 {
			return false
		}
	}
	return ret
}

// Assign worker tasks
func (c *Coordinator) AssignWorker(args *WorkerTaskRequestArgs, reply *WorkerTaskRequestReply) error {

	// Your code here.
	for fileName, status := range c.MapStatus {
		fmt.Println(fileName)
		if status == 0 {
			c.mu.Lock()
			defer c.mu.Unlock()
			c.MapStatus[fileName] = 1 // Mark as in progress

			fmt.Printf("Worker %d assigned task: %s\n", args.WorkerId, fileName)
			reply.TaskName = fileName
			reply.WorkerId = args.WorkerId
			reply.Done = false // Indicating the task is not done
			fmt.Printf("Assigned task %s to worker %d\n", fileName, reply.WorkerId)
			return nil
		}
	}
	return nil
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Your code here.

	c := Coordinator{}
	c.nReducers = nReduce
	c.MapStatus = make(map[string]int)
	c.reduceStatus = make(map[int]int)
	c.intermediateFiles = make(map[int][]string)

	// Initialize mapStatus for each input file.
	for _, file := range files {
		c.MapStatus[file] = 0
	}

	c.server()
	return &c
}
