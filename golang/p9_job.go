package main

import (
	"container/heap"
	"encoding/json"
	"fmt"
	"pyxyne/protohackers/lib"
	"slices"
	"sync"
	"sync/atomic"
)

type Opt[T any] struct {
	Def bool
	Val T
}

func (o *Opt[T]) UnmarshalJSON(data []byte) error {
	o.Def = true
	return json.Unmarshal(data, &o.Val)
}

type P9Req struct {
	Request string
	Queue   Opt[string]
	Job     Opt[any]
	Pri     Opt[uint64]
	Queues  []string
	Wait    bool
	Id      Opt[int64]
}

type Hole[T any] struct {
	filled  atomic.Bool
	Channel chan T
}

func NewHole[T any]() *Hole[T] {
	return &Hole[T]{
		Channel: make(chan T, 1),
	}
}
func (c *Hole[T]) TryFill(val T) bool {
	if c.filled.CompareAndSwap(false, true) {
		c.Channel <- val
		close(c.Channel)
		return true
	}
	return false
}

type Job struct {
	id      int64
	queue   string
	prio    uint64
	payload any
	worker  atomic.Int64
}
type Queue struct {
	mutex   sync.Mutex
	heap    []*Job
	waiting []*Hole[*Job]
}

// sort.Interface
func (q *Queue) Len() int {
	return len(q.heap)
}
func (q *Queue) Less(i, j int) bool {
	return q.heap[i].prio > q.heap[j].prio // Max heap so invert comparison
}
func (q *Queue) Swap(i, j int) {
	tmp := q.heap[i]
	q.heap[i] = q.heap[j]
	q.heap[j] = tmp
}

// heap.Interface
func (q *Queue) Push(x any) {
	j, ok := x.(*Job)
	lib.Assert(ok)
	q.heap = append(q.heap, j)
}
func (q *Queue) Pop() any {
	j := q.heap[len(q.heap)-1]
	q.heap = q.heap[:len(q.heap)-1]
	return j
}

func (q *Queue) Put(j *Job) {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	if len(q.waiting) > 0 {
		for i, h := range q.waiting {
			if h.TryFill(j) {
				q.waiting = slices.Delete(q.waiting, 0, i+1)
				return
			}
		}
		q.waiting = nil
	}
	heap.Push(q, j)
}

func (q *Queue) Delete(jobId int64) {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	for i, j := range q.heap {
		if j.id == jobId {
			heap.Remove(q, i)
			return
		}
	}
}

func Get(queues []*Queue, wait bool, log *lib.Logger) *Job {
	bestI := -1
	prio := uint64(0)
	for i, q := range queues {
		q.mutex.Lock()
		if len(q.heap) > 0 && (i == -1 || q.heap[0].prio > prio) {
			prio = q.heap[0].prio
			bestI = i
		}
	}
	var j *Job
	if bestI != -1 {
		j = heap.Pop(queues[bestI]).(*Job)
	}
	if j == nil && wait {
		log.Debug("Waiting for job...")
		h := NewHole[*Job]()
		for _, q := range queues {
			q.waiting = append(q.waiting, h)
		}
		for _, q := range queues {
			q.mutex.Unlock()
		}
		return <-h.Channel
	} else {
		for _, q := range queues {
			q.mutex.Unlock()
		}
		return j
	}
}

func P9() {
	var prevJobId atomic.Int64
	var prevClientId atomic.Int64
	var mutex sync.RWMutex
	queues := make(map[string]*Queue)
	jobs := make(map[int64]*Job)

	getQueue := func(name string) *Queue {
		mutex.RLock()
		q, ok := queues[name]
		mutex.RUnlock()
		if !ok {
			q = &Queue{}
			mutex.Lock()
			queues[name] = q
			mutex.Unlock()
		}
		return q
	}

	createJob := func(job *Job) {
		mutex.Lock()
		defer mutex.Unlock()
		jobs[job.id] = job
	}
	getJob := func(jobId int64) *Job {
		mutex.RLock()
		defer mutex.RUnlock()
		return jobs[jobId]
	}
	deleteJob := func(jobId int64) bool {
		mutex.Lock()
		defer mutex.Unlock()
		j, ok := jobs[jobId]
		if ok {
			j.worker.Store(0)
			delete(jobs, jobId)
			queues[j.queue].Delete(jobId)
			return true
		} else {
			return false
		}
	}

	lib.ServeTcp(func(c *lib.TcpClient) error {
		clientId := prevClientId.Add(1)
		workedOn := make([]int64, 0)

		invalidRequest := func(extra string) {
			c.Log.Warn("Invalid request: %s", extra)
			c.WriteLine(`{"status":"error","error":"Invalid request"}`)
		}

		abortJob := func(j *Job) bool {
			if j.worker.CompareAndSwap(clientId, 0) {
				q := getQueue(j.queue)
				q.Put(j)
				return true
			} else {
				return false
			}
		}

		for c.HasData() {
			msg, err := c.ReadLine()
			if err != nil {
				return err
			}
			c.Log.Debug("<- %s", msg)

			var req P9Req
			err = json.Unmarshal([]byte(msg), &req)
			if err != nil {
				invalidRequest("unmarshal failed")
				continue
			}

			switch req.Request {
			case "put":
				if !req.Queue.Def || !req.Job.Def || !req.Pri.Def {
					invalidRequest("missing required field")
					continue
				}

				jobId := prevJobId.Add(1)
				j := &Job{
					id:      jobId,
					queue:   req.Queue.Val,
					prio:    req.Pri.Val,
					payload: req.Job.Val,
					worker:  atomic.Int64{},
				}
				createJob(j)
				getQueue(req.Queue.Val).Put(j)

				c.Log.Debug("-> id: %d", jobId)
				c.WriteLine(fmt.Sprintf(`{"status":"ok","id":%d}`, jobId))

			case "get":
				if req.Queues == nil {
					invalidRequest("missing required field")
					continue
				}
				queues := make([]*Queue, len(req.Queues))
				for i, queueName := range req.Queues {
					queues[i] = getQueue(queueName)
				}
				j := Get(queues, req.Wait, &c.Log)
				if j == nil {
					c.Log.Debug("No job")
					c.WriteLine(`{"status":"no-job"}`)
				} else {
					c.Log.Debug("Got job %d", j.id)
					lib.Assert(j.worker.CompareAndSwap(0, clientId))
					workedOn = append(workedOn, j.id)

					bytes, err := json.Marshal(map[string]any{
						"status": "ok",
						"id":     j.id,
						"job":    j.payload,
						"pri":    j.prio,
						"queue":  j.queue,
					})
					lib.CheckPanic(err)
					bytes = append(bytes, '\n')
					c.WriteAll(bytes)
				}

			case "delete":
				if !req.Id.Def {
					invalidRequest("missing required field")
					continue
				}
				if deleteJob(req.Id.Val) {
					c.Log.Debug("Ok")
					c.WriteLine(`{"status":"ok"}`)
				} else {
					c.Log.Debug("No job")
					c.WriteLine(`{"status":"no-job"}`)
				}

			case "abort":
				if !req.Id.Def {
					invalidRequest("missing required field")
					continue
				}
				j := getJob(req.Id.Val)
				if j == nil {
					c.Log.Debug("No job")
					c.WriteLine(`{"status":"no-job"}`)
				} else if abortJob(j) {
					c.Log.Debug("Job added back to queue")
					c.WriteLine(`{"status":"ok"}`)
				} else {
					c.Log.Error("Not own job")
					c.WriteLine(`{"status":"error","error":"Not working on this job"}`)
				}

			default:
				invalidRequest("invalid request type")
			}
		}

		for _, jobId := range workedOn {
			j := getJob(jobId)
			if j != nil && abortJob(j) {
				c.Log.Debug("Cancelled job %d", jobId)
			}
		}

		return nil
	})
}
