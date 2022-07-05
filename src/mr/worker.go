package mr

import (
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"net/rpc"
	"os"
	"strings"
	"time"
)

var DEBUG bool = false

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

func check(e error) {
	if e != nil {
		if DEBUG {
			fmt.Print(e)
		}
		os.Exit(0)
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
	exit_counter := 0
	for {
		if exit_counter > 8 {
			return
		}
		reply, err := RequestForTask()
		if err != nil {
			if DEBUG {
				fmt.Println("request task failed")
			}
			return
		}
		if reply.TaskType == 0 {
			exit_counter = 0
			if DEBUG {
				fmt.Printf("Get map task %v\n", reply.Index)
			}
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
			exit_counter = 0
			if DEBUG {
				fmt.Printf("Get reduce task %v\n", reply.Index)
			}
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
			if DEBUG {
				fmt.Printf("Stay idle\n")
			}
			exit_counter++
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
		if DEBUG {
			fmt.Printf("request task failed!\n")
		}
		return RequestTaskReply{}, errors.New("RPC call failed")
	}
	return reply, nil
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
		// log.Fatal("dialing:", err)
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}
	if DEBUG {
		fmt.Println(err)
	}
	return false
}
