package mr

import (
	"bufio"
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

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
	for {
		task, ok := GetTaskFromCoordinator()
		if !ok {
			log.Default().Println("GetTask failed")
			return
		}

		if task.TaskType == TaskType_Map {
			if task.FileName == "" {
				time.Sleep(5 * time.Second)
				continue
			}

			file, err := os.Open(task.FileName)
			if err != nil {
				log.Default().Printf("cannot open %v\n", task.FileName)
				if _, ok := ReportTaskCoordinator(TaskType_Map, task.FileName, task.Reduce, task.TaskIndex, Task_Status_ERROR); !ok {
					log.Default().Printf("Can not report task to coordinator\n")
				}
				return
			}

			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", task.FileName)

				if _, ok := ReportTaskCoordinator(TaskType_Map, task.FileName, task.Reduce, task.TaskIndex, Task_Status_ERROR); !ok {
					log.Default().Printf("Can not report task to coordinator\n")
				}
				return
			}
			file.Close()

			kva := mapf(task.FileName, string(content))

			reduce := int(1)
			if task.Reduce > 0 {
				reduce = int(task.Reduce)
			}

			var listInter []*os.File
			err = nil
			var fileOut *os.File
			for i := 0; i < reduce; i++ {
				fileOut, err = os.Create(fmt.Sprintf("mr-%v-%v", task.TaskIndex, i))
				if err != nil {
					log.Default().Printf("Error while create output file %s\n", task.FileName)
					continue
				}
				listInter = append(listInter, fileOut)
			}

			if err != nil {
				if _, ok := ReportTaskCoordinator(TaskType_Map, task.FileName, task.Reduce, task.TaskIndex, Task_Status_ERROR); !ok {
					log.Default().Printf("Can not report task to coordinator\n")
				}
				continue
			}

			for _, v := range kva {
				idx := ihash(v.Key) % reduce
				listInter[idx].WriteString(fmt.Sprintf("%v %v\n", v.Key, v.Value))
			}

			for i := 0; i < reduce; i++ {
				listInter[i].Close()
			}

			if _, ok := ReportTaskCoordinator(TaskType_Map, task.FileName, task.Reduce, task.TaskIndex, Task_Status_OK); !ok {
				log.Default().Printf("Can not report task to coordinator\n")
			}
		} else if task.TaskType == TaskType_Reduce {
			log.Default().Printf("Task Reduce: %v \n", task.Reduce)

			intermediate := []KeyValue{}
			var err error
			var file *os.File
			for i := 0; i < task.NumFiles; i++ {
				fileName := fmt.Sprintf("mr-%v-%v", i, task.Reduce)
				file, err = os.Open(fileName)
				if err != nil {
					log.Default().Printf("cannot open %v\n", task.FileName)
					continue
				}

				scanner := bufio.NewScanner(file)

				// Iterate over each line in the file
				for scanner.Scan() {
					// Split the line into two strings
					parts := strings.Split(scanner.Text(), " ")
					if len(parts) != 2 {
						fmt.Println("Error: Invalid line format")
						err = errors.New("Error: Invalid line format")
						continue
					}

					intermediate = append(intermediate, KeyValue{
						parts[0],
						parts[1],
					})
				}
			}

			if err != nil {
				if _, ok := ReportTaskCoordinator(TaskType_Reduce, task.FileName, task.Reduce, task.TaskIndex, Task_Status_ERROR); !ok {
					log.Default().Printf("Can not report task to coordinator\n")
				}
				continue
			}

			sort.Sort(ByKey(intermediate))

			oname := fmt.Sprintf("mr-out-%v", task.Reduce)
			ofile, _ := os.Create(oname)

			//
			// call Reduce on each distinct key in intermediate[],
			// and print the result to mr-out-0.
			//
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

			if _, ok := ReportTaskCoordinator(TaskType_Reduce, task.FileName, task.Reduce, task.TaskIndex, Task_Status_OK); !ok {
				log.Default().Printf("Can not report task to coordinator\n")
			}
		}

		time.Sleep(1 * time.Second)
	}
}

func GetTaskFromCoordinator() (TaskReply, bool) {
	args := TaskArgs{}
	reply := TaskReply{}
	ok := call("Coordinator.TaskRequest", &args, &reply)
	return reply, ok
}

func ReportTaskCoordinator(taskType int32, filename string, reduce int32, taskIndex int, status int32) (TaskStatus, bool) {
	log.Default().Printf("Send Report %v\n", status)
	args := TaskStatusArg{
		taskType,
		filename,
		reduce,
		taskIndex,
		status,
	}
	reply := TaskStatus{
		Status: status,
	}
	ok := call("Coordinator.TaskReport", &args, &reply)
	return reply, ok
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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
