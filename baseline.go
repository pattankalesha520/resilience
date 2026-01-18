package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type Container struct {
	ID      int
	NodeID  int
	Running bool
	Stop    chan struct{}
	mu      sync.Mutex
}

func NewContainer(id, nodeID int) *Container {
	return &Container{ID: id, NodeID: nodeID, Running: true, Stop: make(chan struct{})}
}

func (c *Container) run(failProb float64) {
	for {
		select {
		case <-c.Stop:
			return
		default:
			time.Sleep(time.Duration(200+rand.Intn(400)) * time.Millisecond)
			if rand.Float64() < failProb {
				c.mu.Lock()
				c.Running = false
				c.mu.Unlock()
				return
			}
		}
	}
}

type Node struct {
	ID         int
	Containers map[int]*Container
	mu         sync.Mutex
}

func NewNode(id int) *Node {
	return &Node{ID: id, Containers: make(map[int]*Container)}
}

type Orchestrator struct {
	Nodes            map[int]*Node
	nextContainerID  int
	failProb         float64
	checkInterval    time.Duration
	failureCount     int
	totalRepairTime  time.Duration
	mu               sync.Mutex
	wg               sync.WaitGroup
}

func NewOrchestrator(failProb float64, checkInterval time.Duration) *Orchestrator {
	return &Orchestrator{Nodes: make(map[int]*Node), failProb: failProb, checkInterval: checkInterval}
}

func (o *Orchestrator) AddNode(n *Node) {
	o.mu.Lock()
	o.Nodes[n.ID] = n
	o.mu.Unlock()
}

func (o *Orchestrator) DeployContainer(nodeID int) {
	o.mu.Lock()
	o.nextContainerID++
	cid := o.nextContainerID
	o.mu.Unlock()
	o.mu.Lock()
	node := o.Nodes[nodeID]
	o.mu.Unlock()
	cont := NewContainer(cid, nodeID)
	node.mu.Lock()
	node.Containers[cid] = cont
	node.mu.Unlock()
	o.wg.Add(1)
	go func() {
		defer o.wg.Done()
		cont.run(o.failProb)
	}()
}

func (o *Orchestrator) monitorOnce() {
	o.mu.Lock()
	nodes := make([]*Node, 0, len(o.Nodes))
	for _, n := range o.Nodes {
		nodes = append(nodes, n)
	}
	o.mu.Unlock()
	for _, n := range nodes {
		n.mu.Lock()
		containers := make([]*Container, 0, len(n.Containers))
		for _, c := range n.Containers {
			containers = append(containers, c)
		}
		n.mu.Unlock()
		for _, c := range containers {
			c.mu.Lock()
			running := c.Running
			c.mu.Unlock()
			if !running {
				start := time.Now()
				o.handleFailure(n, c)
				repair := time.Since(start)
				o.mu.Lock()
				o.failureCount++
				o.totalRepairTime += repair
				o.mu.Unlock()
			}
		}
	}
}

func (o *Orchestrator) handleFailure(n *Node, c *Container) {
	n.mu.Lock()
	_, exists := n.Containers[c.ID]
	if exists {
		delete(n.Containers, c.ID)
	}
	n.mu.Unlock()
	time.Sleep(500 * time.Millisecond)
	o.DeployContainer(n.ID)
}

func (o *Orchestrator) StartMonitoring(stop chan struct{}) {
	ticker := time.NewTicker(o.checkInterval)
	for {
		select {
		case <-ticker.C:
			o.monitorOnce()
		case <-stop:
			ticker.Stop()
			return
		}
	}
}

func (o *Orchestrator) PrintStats() {
	o.mu.Lock()
	defer o.mu.Unlock()
	avgRepair := time.Duration(0)
	if o.failureCount > 0 {
		avgRepair = o.totalRepairTime / time.Duration(o.failureCount)
	}
	fmt.Printf("Failures: %d | Avg Repair: %v\n", o.failureCount, avgRepair)
}

func main() {
	rand.Seed(time.Now().UnixNano())
	orch := NewOrchestrator(0.08, 2*time.Second)
	for i := 1; i <= 5; i++ {
		n := NewNode(i)
		orch.AddNode(n)
		for j := 0; j < 4; j++ {
			orch.DeployContainer(i)
		}
	}
	stop := make(chan struct{})
	go orch.StartMonitoring(stop)
	report := time.NewTicker(5 * time.Second)
	runFor := time.After(40 * time.Second)
loop:
	for {
		select {
		case <-report.C:
			orch.PrintStats()
		case <-runFor:
			close(stop)
			break loop
		}
	}
	orch.wg.Wait()
	orch.PrintStats()
}
