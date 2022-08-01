package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
  TaskStatusReady = iota
  TaskStatusQueue
  TaskStatusRunning
  TaskStatusFinish
  TaskStatusError
)

const (
  MaxTaskRunTime = time.Second * 5
  ScheduleInterval = time.Millisecond * 500
)

type TaskState struct {
  status int
  workerId int
  startTime time.Time
}

type TaskPhase int

const (
  MapPhase TaskPhase = 0
  ReducePhase TaskPhase = 1
)

// this struct need serialize/deserialize in RPC
// so we have to declare all fields public
type Task struct {
  File string
  Phase TaskPhase
  Alive bool

  // task identity, means the index in the file slices
  // maybe the worker timeout, cannot reply the response 
  // so one task maybe sent to multiple different workers
  TaskId int

  // automatic increasing counter, means the worker identity
  // if the old worker report the result again, we can ignore it
  WorkerId int

  NReduce int
  NMap int
}

type Master struct {
  mux sync.Mutex
  files []string
  nReduce int
  taskPhase TaskPhase
  taskStats []TaskState

  workerId int

  taskCh chan Task
  done bool
}

func (m *Master) initMapTask() {
  m.taskPhase = MapPhase
  m.taskStats = make([]TaskState, len(m.files))
}

func (m *Master) initReduceTask() {
  m.taskPhase = ReducePhase
  m.taskStats = make([]TaskState, m.nReduce)
}

func (m *Master) finished() bool {
  m.mux.Lock()
  defer m.mux.Unlock()
  return m.done
}

func (m *Master) tick() {
  for !m.finished() {
    go m.schedule()
    time.Sleep(ScheduleInterval)
  }
}

func (m *Master) schedule() {
  m.mux.Lock()
  defer m.mux.Unlock()

  if m.done {
    return
  }

  finished := true
  for i, stat := range m.taskStats {
    switch(stat.status) {
    case TaskStatusReady:
      finished = false
      m.taskCh <- m.getTask(i)
      m.taskStats[i].status = TaskStatusQueue
    case TaskStatusQueue:
      finished = false
    case TaskStatusRunning:
      finished = false
      if time.Now().Sub(stat.startTime) > MaxTaskRunTime {
        m.taskStats[i].status = TaskStatusQueue
        m.taskCh <- m.getTask(i)
      }
    case TaskStatusFinish:
    case TaskStatusError:
      finished = false
      m.taskStats[i].status = TaskStatusQueue
      m.taskCh <- m.getTask(i)
    default:
      panic("unknown task status")
    }
  }
  if finished {
    if m.taskPhase == MapPhase {
      m.initReduceTask()
    } else {
      m.done = true
    }
  }
}

func (m *Master) getTask(tid int) Task {
  task := Task {
    File: "",
    TaskId: tid,
    Phase: m.taskPhase,
    Alive: true,
    NReduce: m.nReduce,
    NMap: len(m.files),
  }
  if task.Phase == MapPhase {
    task.File = m.files[tid]
  }
  return task
}

func (m *Master) ReportTask(req *ReportTaskReq, reply *ReportTaskReply) error {
  m.mux.Lock();
  defer m.mux.Unlock()

  // maybe this task report is outdated
  if m.taskPhase != req.Phase || m.taskStats[req.TaskId].workerId != req.WorkerId {
    return nil
  }

  if req.Done {
    m.taskStats[req.TaskId].status = TaskStatusFinish
  } else {
    m.taskStats[req.TaskId].status = TaskStatusError
  }

  return nil
}

func (m *Master) GetOneTask(req *TaskReq, reply *TaskReply) error {
  task := <-m.taskCh
  reply.Task = &task

  if task.Alive {
    m.regTask(&task)
  }

  return nil
}

func (m *Master) regTask(task *Task) {
  m.mux.Lock()
  defer m.mux.Unlock()

  if task.Phase != m.taskPhase {
    panic("invalid task phase")
  }

  m.workerId++
  task.WorkerId = m.workerId

  m.taskStats[task.TaskId].status = TaskStatusRunning
  m.taskStats[task.TaskId].workerId = m.workerId
  m.taskStats[task.TaskId].startTime = time.Now()
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
  return m.finished()
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

  m.mux = sync.Mutex{}
  m.nReduce = nReduce
  m.files = files

  if m.nReduce > len(files) {
    m.taskCh = make(chan Task, nReduce)
  } else {
    m.taskCh = make(chan Task, len(m.files))
  }

  m.initMapTask()
  go m.tick()

	m.server()
  log.Printf("master init");
	return &m
}
