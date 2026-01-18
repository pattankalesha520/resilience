package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type C struct {
	ID      int
	Svc     string
	NodeID  int
	Running bool
	State   int
	Replica bool
	mu      sync.Mutex
	stop    chan struct{}
}

func NewC(id int, svc string, node int, rep bool) *C {
	c := &C{ID: id, Svc: svc, NodeID: node, Running: true, State: rand.Intn(1000), Replica: rep, stop: make(chan struct{})}
	go func(cc *C) {
		for {
			select {
			case <-cc.stop:
				return
			default:
				time.Sleep(time.Duration(200+rand.Intn(300)) * time.Millisecond)
				cc.mu.Lock()
				if !cc.Replica && rand.Float64() < 0.02 {
					cc.Running = false
					cc.mu.Unlock()
					return
				}
				cc.State += rand.Intn(8)
				cc.mu.Unlock()
			}
		}
	}(c)
	return c
}

type N struct {
	ID         int
	Cap, Used  int
	Containers map[int]*C
	mu         sync.Mutex
}

func NewN(id, cap int) *N { return &N{ID: id, Cap: cap, Containers: make(map[int]*C)} }

type Cluster struct {
	Nodes map[int]*N
	next  int
	mu    sync.Mutex
}

func NewCluster() *Cluster { return &Cluster{Nodes: make(map[int]*N)} }

func (cl *Cluster) AddNode(cap int) {
	cl.mu.Lock()
	id := len(cl.Nodes) + 1
	cl.Nodes[id] = NewN(id, cap)
	cl.mu.Unlock()
}

func (cl *Cluster) Deploy(svc string) *C {
	cl.mu.Lock()
	cl.next++
	cid := cl.next
	var tgt *N
	for _, n := range cl.Nodes {
		n.mu.Lock()
		if n.Used+1 <= n.Cap {
			tgt = n
			n.mu.Unlock()
			break
		}
		n.mu.Unlock()
	}
	if tgt == nil {
		for _, n := range cl.Nodes { tgt = n; break }
	}
	c := NewC(cid, svc, tgt.ID, false)
	tgt.mu.Lock()
	tgt.Containers[cid] = c
	tgt.Used++
	tgt.mu.Unlock()
	cl.mu.Unlock()
	return c
}


func (cl *Cluster) Snapshot() (int, int) {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	total, used := 0, 0
	for _, n := range cl.Nodes {
		n.mu.Lock()
		total += n.Cap
		used += n.Used
		n.mu.Unlock()
	}
	return used, total
}

type EMA struct{ a, v float64; init bool; mu sync.Mutex }

func NewEMA(a float64) *EMA { return &EMA{a: a} }
func (e *EMA) Update(x float64) { e.mu.Lock(); if !e.init { e.v = x; e.init = true } else { e.v = e.a*x + (1-e.a)*e.v }; e.mu.Unlock() }
func (e *EMA) Value() float64  { e.mu.Lock(); v := e.v; e.mu.Unlock(); return v }

type Monitor struct {
	cl      *Cluster
	lat, fl *EMA
}

func NewMonitor(c *Cluster) *Monitor { return &Monitor{cl: c, lat: NewEMA(0.4), fl: NewEMA(0.3)} }

func (m *Monitor) Collect() (float64, float64, int) {
	used, tot := m.cl.Snapshot()
	util := 0.0
	if tot > 0 { util = float64(used) / float64(tot) * 100 }
	fail := 0
	m.cl.mu.Lock()
	for _, n := range m.cl.Nodes {
		n.mu.Lock()
		for _, c := range n.Containers {
			c.mu.Lock()
			if !c.Running && !c.Replica { fail++ }
			c.mu.Unlock()
		}
		n.mu.Unlock()
	}
	m.cl.mu.Unlock()
	est := 100 + util*1.5
	m.lat.Update(est)
	m.fl.Update(float64(fail))
	return util, m.lat.Value(), fail
}

type Orchestrator struct {
	cl *Cluster
	m  *Monitor
}


func main() {
	rand.Seed(time.Now().UnixNano())
	cl := NewCluster()
	for i := 0; i < 3; i++ { cl.AddNode(10) }
	orch := NewOrch(cl)
	svcs := []string{"auth","api","db","cache","worker"}
	for i := 0; i < 9; i++ { cl.Deploy(svcs[i%len(svcs)]) }
	go func() {
		for range time.Tick(1 * time.Second) { orch.Act() }
	}()
	go func() {
		for range time.Tick(400 * time.Millisecond) {
			cl.mu.Lock()
			for _, n := range cl.Nodes {
				n.mu.Lock()
				for _, c := range n.Containers {
					c.mu.Lock()
					if !c.Running && !c.Replica { c.mu.Unlock(); continue }
					if rand.Float64() < 0.01 && !c.Replica { c.Running = false }
					c.mu.Unlock()
				}
				n.mu.Unlock()
			}
			cl.mu.Unlock()
		}
	}()
	tick := time.NewTicker(2 * time.Second)
	stop := time.After(40 * time.Second)
loop:
	for {
		select {
		case <-tick.C:
			u, t := cl.Snapshot()
			fmt.Printf("Nodes:%d Used:%d Total:%d\n", len(cl.Nodes), u, t)
		case <-stop:
			break loop
		}
	}
}
