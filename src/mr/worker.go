package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	reply := GetTaskInfo()
	if reply.Done {
		fmt.Printf("Worker %d has no tasks assigned.\n", reply.WorkerId)
		return
	}
	filename := reply.TaskName

	//All Okay till here
	PerformMapTask(filename)
}

func PerformMapTask(filename string) {
	file, err := os.Open(filename)
	intermediate := []KeyValue{}
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)

	fmt.Printf("Worker %d processed file: %s\n", reply.WorkerId, filename)
	sort.Sort(ByKey(intermediate))

	oname := "mr-out-0"
	ofile, _ := os.Create(oname)
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	ofile.Close()
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	// args := ExampleArgs{}
	args := WorkerTaskRequestArgs{}

	// fill in the argument(s).
	args.WorkerId = 99

	// declare a reply structure.
	// reply := ExampleReply{}
	reply := WorkerTaskRequestReply{}
	reply.WorkerId = args.WorkerId
	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.WorkerAssignment", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Done)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// call an RPC to request for tasks
func GetTaskInfo() WorkerTaskRequestReply {
	args := WorkerTaskRequestArgs{}
	args.WorkerId = os.Getpid()
	reply := WorkerTaskRequestReply{}
	ok := call("Coordinator.AssignWorker", &args, &reply)
	if ok {
		fmt.Printf("Worker %d assigned task: %s\n", reply.WorkerId, reply.TaskName)
		return reply
	}
	fmt.Printf("Failed to get task info for worker %d\n", args.WorkerId)
	return reply
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
