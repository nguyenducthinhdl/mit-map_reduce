package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	Status_Done    = int32(0)
	Status_Idle    = int32(1)
	Status_Process = int32(2)

	Phase_Map    int32 = 0
	Phase_Reduce int32 = 1

	TimeOut = 5 * time.Second
)

type Task struct {
	Status   int32
	RIndex   int
	FileName string

	StartTime time.Time
}

type Coordinator struct {
	// Your definitions here.
	MapTasks    map[int]Task
	ReduceTasks map[int]Task
	Reduce      int32
	Phase       int32
	done        atomic.Bool
	Filenames   []string
	Remain      int

	mu sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) TaskRequest(args *TaskArgs, reply *TaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.Phase == Phase_Map {
		fmt.Printf("[TaskRequest] beginning with size of task: %v\n", len(c.MapTasks))
		// Race condition and complex
		for k, v := range c.MapTasks {
			if v.Status == Status_Idle ||
				(v.Status == Status_Process && time.Now().Add(-TimeOut).After(v.StartTime)) {
				reply.FileName = v.FileName
				reply.TaskType = TaskType_Map
				reply.Reduce = c.Reduce
				reply.TaskIndex = v.RIndex
				reply.NumFiles = len(c.Filenames)
				c.MapTasks[k] = Task{
					Status_Process,
					v.RIndex,
					v.FileName,
					time.Now(),
				}
				return nil
			}
		}

		reply.TaskType = TaskType_Map
		reply.FileName = ""
	} else if c.Phase == Phase_Reduce {
		for k, v := range c.ReduceTasks {
			if v.Status == Status_Idle ||
				(v.Status == Status_Process && time.Now().Add(-TimeOut).After(v.StartTime)) {
				reply.FileName = strings.Join(c.Filenames, ",")
				reply.TaskType = TaskType_Reduce
				reply.Reduce = int32(v.RIndex)
				reply.TaskIndex = v.RIndex
				reply.NumFiles = len(c.Filenames)
				c.ReduceTasks[k] = Task{
					Status_Process,
					v.RIndex,
					"",
					time.Now(),
				}
				return nil
			}
		}
	}

	return nil
}

func (c *Coordinator) TaskReport(args *TaskStatusArg, reply *TaskStatus) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	log.Default().Printf("Task Report %v\n", args)

	if args.TaskType == TaskType_Map && c.Phase == Phase_Map {
		if v, ok := c.MapTasks[args.TaskIndex]; ok {
			if args.Status == Task_Status_OK {
				delete(c.MapTasks, args.TaskIndex)
				if len(c.MapTasks) == 0 {
					log.Default().Printf("Change to Reduce Phase \n")
					c.Phase = TaskType_Reduce
					c.Remain = len(c.ReduceTasks)
				}
			} else if args.Status == Task_Status_ERROR {
				c.MapTasks[args.TaskIndex] = Task{
					Status:   Status_Idle,
					RIndex:   v.RIndex,
					FileName: v.FileName,
				}
			}
		}
	} else if args.TaskType == TaskType_Reduce && c.Phase == Phase_Reduce {
		if args.Status == Task_Status_OK && c.ReduceTasks[int(args.Reduce)].Status == Status_Process {
			c.ReduceTasks[int(args.Reduce)] = Task{
				Status: Status_Done,
			}
			c.Remain--
			if c.Remain == 0 {
				// need implement
				log.Default().Printf("Change to Done \n")
				c.done.Store(true)
			}
		} else if reply.Status == Task_Status_ERROR &&
			c.ReduceTasks[int(c.Reduce)].Status == Status_Process {
			c.ReduceTasks[int(args.Reduce)] = Task{
				Status: Status_Idle,
				RIndex: int(c.Reduce),
			}
		}

	}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	if c.done.Load() {
		log.Default().Printf("Done !!!\n")
	}
	return c.done.Load()
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.MapTasks = map[int]Task{}
	c.ReduceTasks = map[int]Task{}
	c.Reduce = int32(nReduce)
	c.done.Store(false)

	for i := 0; i < nReduce; i++ {
		c.ReduceTasks[i] = Task{
			Status: Status_Idle,
			RIndex: i,
		}
	}

	var mapIndexTask int = 0
	for _, filename := range os.Args[1:] {
		// filename = strings.TrimPrefix(filename, "../")
		// fmt.Println(filename)
		c.MapTasks[mapIndexTask] = Task{
			FileName: filename,
			Status:   Status_Idle,
			RIndex:   mapIndexTask,
		}
		mapIndexTask++
		c.Filenames = append(c.Filenames, filename)
	}

	fmt.Printf("[MakeCoordinator] Tasks: %v\n", len(c.MapTasks))
	c.Phase = Phase_Map

	c.server()
	return &c
}
