package mr

import (
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strings"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

func check(e error) {
	if e != nil {
		panic(e)
	}
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

func WriteKVToFile(fileName string, kva []KeyValue) {
	f, err := os.Create(fileName)
	check(err)
	defer f.Close()
	for _, kv := range kva {
		_, err := f.WriteString(kv.Key + " " + kv.Value + "\n")
		check(err)
	}
}

func ReadFile(fileName string) []KeyValue {
	res := make([]KeyValue, 0)
	f, err := os.Open(fileName)
	check(err)
	oldStdin := os.Stdin
	os.Stdin = f
	for {
		kv := KeyValue{}
		_, err := fmt.Scanf("%s %s", &kv.Key, &kv.Value)
		if err != nil || len(strings.TrimSpace(kv.Key)) == 0 {
			break
		}
		res = append(res, kv)
	}
	os.Stdin = oldStdin
	f.Close()
	return res
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	os.MkdirAll("./temp", os.ModePerm)
	for {
		reply, err := RequestForTask()
		if err != nil {
			fmt.Println("request task failed")
			continue
		}
		if reply.TaskType == 0 {
			fmt.Printf("Get map task %v\n", reply.Index)
			content, _ := ioutil.ReadFile(reply.MapFileName)
			mapped := mapf(reply.MapFileName, string(content))
			mat := make([][]KeyValue, reply.NReduce)
			for _, kv := range mapped {
				idx := ihash(kv.Key) % reply.NReduce
				mat[idx] = append(mat[idx], kv)
			}

			check(err)
			for i := 0; i < reply.NReduce; i++ {
				WriteKVToFile(fmt.Sprintf("./temp/%v %v.mapped", reply.Index, i), mat[i])
				// WriteToFile("./temp/"+fmt.Sprint(reply.Index)+" "+fmt.Sprint(i)+".mapped", mat[i])
			}
			ReportFinish(reply.TaskType, reply.Index)
		} else if reply.TaskType == 1 {
			fmt.Printf("Get reduce task %v\n", reply.Index)
			merged := make(map[string]([]string))
			for i := 0; i < reply.NMap; i++ {
				kva := ReadFile(fmt.Sprintf("./temp/%v %v.mapped", i, reply.Index))
				for _, kv := range kva {
					_, prs := merged[kv.Key]
					if !prs {
						merged[kv.Key] = make([]string, 0)

					}
					merged[kv.Key] = append(merged[kv.Key], kv.Value)
				}
			}
			kva := make([]KeyValue, 0)
			for k, v := range merged {
				kva = append(kva, KeyValue{k, reducef(k, v)})
			}
			// fmt.Print(count)
			WriteKVToFile(fmt.Sprintf("mr-out-%v", reply.Index), kva)
			ReportFinish(reply.TaskType, reply.Index)
		} else {
			fmt.Printf("Stay idle\n")
		}
		time.Sleep(time.Second)
	}
}

func ReportFinish(taskType int, idx int) {
	args := TaskDoneArgs{}
	args.Index = idx
	args.TaskType = taskType
	reply := TaskDoneReply{}
	ok := false
	for !ok {
		ok = call("Coordinator.Finish", &args, &reply)
	}
}

func RequestForTask() (RequestTaskReply, error) {
	args := RequestTask{}
	reply := RequestTaskReply{}
	ok := call("Coordinator.RequestTask", &args, &reply)
	if !ok {
		fmt.Printf("request task failed!\n")
		return RequestTaskReply{}, errors.New("RPC call failed")
	}
	return reply, nil
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
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
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
