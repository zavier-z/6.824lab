package mr

import "os"
import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "encoding/json"
import "io/ioutil"
import "strings"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
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
  w := worker {
    mapf: mapf,
    reducef: reducef,
  }

  w.run()
}

type worker struct {
  mapf func(string, string) []KeyValue
  reducef func(string, []string) string
}

func (w *worker) run() {
  for {
    t := w.reqTask()
    if !t.Alive {
      return
    }
    w.doTask(t)
  }
}

func (w *worker) reqTask() Task {
  req := TaskReq{}
  reply := TaskReply{}

  if ok := call("Master.GetOneTask", &req, &reply); !ok {
    log.Printf("worker get task failed, exit");
    os.Exit(1)
  }
  return *reply.Task
}

func (w *worker) doTask(t Task) {
  switch t.Phase {
    case MapPhase:
      w.doMapTask(t)
    case ReducePhase:
      w.doReduceTask(t)
    default:
      panic("task phase error")
  }
}

func (w *worker) doMapTask(t Task) {
  contexts, err := ioutil.ReadFile(t.File)
  if err != nil {
    w.reportTask(t, false, err)
    return
  }

  kvs := w.mapf(t.File, string(contexts))
  reduces := make([][]KeyValue, t.NReduce)
  for _, kv := range kvs {
    idx := ihash(kv.Key) % t.NReduce
    reduces[idx] = append(reduces[idx], kv)
  }

  for idx, l := range reduces {
    file := reduceName(t.TaskId, idx)
    f, err := os.Create(file)
    if err != nil {
      w.reportTask(t, false, err)
      return
    }

    enc := json.NewEncoder(f)
    for _, kv := range l {
      if err := enc.Encode(&kv); err != nil {
        w.reportTask(t, false, err)
        return
      }
    }
    if err := f.Close(); err != nil {
      w.reportTask(t, false, err)
      return
    }
  }

  w.reportTask(t, true, nil)
}

func (w *worker) doReduceTask(t Task) {
  maps := make(map[string][]string)
  for idx := 0; idx < t.NMap; idx++ {
    filename := reduceName(idx, t.TaskId)
    file, err := os.Open(filename)
    if err != nil {
      w.reportTask(t, false, err)
      return
    }

    dec := json.NewDecoder(file)
    for {
      var kv KeyValue
      if err := dec.Decode(&kv); err != nil {
        break
      }

      if _, ok := maps[kv.Key]; !ok {
        maps[kv.Key] = make([]string, 0, 100)
      }

      maps[kv.Key] = append(maps[kv.Key], kv.Value)
    }
  }

  res := make([]string, 0, 100)
  for k, v := range maps {
    res = append(res, fmt.Sprintf("%v %v\n", k, w.reducef(k, v)))
  }

  if err := ioutil.WriteFile(mergeName(t.TaskId),
    []byte(strings.Join(res, "")), 0600); err != nil {
    w.reportTask(t, false, err)
  }

  w.reportTask(t, true, nil)
}

func (w *worker) reportTask(t Task, done bool, err error) {
  if err != nil {
    log.Printf("%v", err)
  }

  req := ReportTaskReq {
    Done: done,
    Phase: t.Phase,
    TaskId: t.TaskId,
    WorkerId: t.WorkerId,
  }

  reply := ReportTaskReply{}

  if ok := call("Master.ReportTask", &req, &reply); !ok {
    log.Printf("report failed: %v", req)
  }
}

func reduceName(mapIdx, reduceIdx int) string {
  return fmt.Sprintf("mr-%d-%d", mapIdx, reduceIdx)
}

func mergeName(reduceIdx int) string {
  return fmt.Sprintf("mr-out-%d", reduceIdx)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
