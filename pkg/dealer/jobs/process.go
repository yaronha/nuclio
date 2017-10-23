/*
Copyright 2017 The Nuclio Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package jobs

import (
	"time"
	"fmt"
	"github.com/pkg/errors"
	"github.com/nuclio/nuclio-sdk"
	"github.com/nuclio/nuclio/pkg/dealer/client"
	"net/http"
	"github.com/yaronha/kubetest/xendor/k8s.io/client-go/pkg/util/json"
)

type ProcessState int8

const (
	ProcessStateUnkown    ProcessState = 0
	ProcessStateReady     ProcessState = 1
	ProcessStateNotReady  ProcessState = 2
	ProcessStateDelete    ProcessState = 3
)


type BaseProcess struct {
	Name          string                `json:"name"`
	Namespace     string                `json:"namespace"`
	Function      string                `json:"function"`
	Version       string                `json:"version,omitempty"`
	Alias         string                `json:"alias,omitempty"`
	IP            string                `json:"ip"`
	Port          int                   `json:"port"`
	State         ProcessState          `json:"state"`
	LastUpdate    time.Time             `json:"lastUpdate,omitempty"`
}

type Process struct {
	Name          string                `json:"name"`
	Namespace     string                `json:"namespace"`
	Function      string                `json:"function"`
	Version       string                `json:"version,omitempty"`
	Alias         string                `json:"alias,omitempty"`
	IP            string                `json:"ip"`
	Port          int                   `json:"port"`
	State         ProcessState          `json:"state"`
	LastUpdate    time.Time             `json:"lastUpdate,omitempty"`

	//BaseProcess
	logger        nuclio.Logger
	ctx           *ManagerContext
	removingJob   bool
	job           *Job
	tasks         []*Task
}

type ProcessMessage struct {
	Name          string                `json:"name"`
	Namespace     string                `json:"namespace"`
	Function      string                `json:"function"`
	Version       string                `json:"version,omitempty"`
	Alias         string                `json:"alias,omitempty"`
	IP            string                `json:"ip"`
	Port          int                   `json:"port"`
	State         ProcessState          `json:"state"`
	LastUpdate    time.Time             `json:"lastUpdate,omitempty"`

	TotalTasks    int                   `json:"totalTasks,omitempty"`
	Metadata      interface{}           `json:"metadata,omitempty"`
	Tasks         []Task                `json:"tasks,omitempty"`
}

func (p *ProcessMessage) Render(w http.ResponseWriter, r *http.Request) error {
	return nil
}



func NewProcess(logger nuclio.Logger, context *ManagerContext, proc *Process) (*Process, error) {
	if proc.Namespace == "" {
		proc.Namespace = "default"
	}
	proc.LastUpdate = time.Now()
	proc.ctx = context
	proc.logger = logger
	return proc, nil
}

func ProcessKey(name,namespace string) string {return name+"."+namespace}

func (p *Process) AsString() string {
	return fmt.Sprintf("%s-%s:%s",p.Name,p.job.Name,p.tasks)
}

// force remove a process: mark its tasks unassigned, remove from job, rebalance (assign the tasks to other procs)
func (p *Process) Remove() error {

	for _, task := range p.tasks {
		task.State = TaskStateUnassigned
		task.SetProcess(nil)
		task.LastUpdate = time.Now()
	}

	if p.job == nil {
		return nil
	}

	p.removingJob = true
	delete(p.job.Processes, p.Name)
	err := p.job.Rebalance()
	p.job = nil
	return err
}

// assign a process to a job
func (p *Process) SetJob(job *Job) error {
	if p.job != nil {
		return fmt.Errorf("Process already assigned a job, use clear job method first")
	}
	p.job = job
	job.Processes[p.Name] = p
	return nil
}

// Request to stop all process tasks, clear Job assosiation if no tasks
func (p *Process) ClearJob() error {
	if p.job == nil {
		return nil
	}
	if len(p.tasks) == 0 {
		delete(p.job.Processes, p.Name)
		p.job = nil
		return nil
	}
	p.removingJob = true

	for _, task := range p.tasks {
		task.State = TaskStateStopping
	}

	return p.PushUpdates()
}

// return list of tasks assigned to this proc
func (p *Process) GetTasks(active bool) []*Task {
	list := []*Task{}
	for _, task := range p.tasks {
		if !active || task.State != TaskStateStopping {
			list = append(list, task)
		}
	}
	return list
}

// add list of tasks to process
func (p *Process) AddTasks(tasks []*Task) {
	for _, task := range tasks {
		task.State = TaskStateAlloc
		task.SetProcess(p)
		task.LastUpdate = time.Now()
	}

	p.tasks = append(p.tasks, tasks...)

}

// remove specific task from proc
func (p *Process) RemoveTask(id int) {
	for i, task := range p.tasks {
		if task.Id == id {
			p.tasks = append(p.tasks[:i], p.tasks[i+1:]...)
		}
	}
}

func (p *Process) StopNTasks(toDelete int) {
	if toDelete <= 0 {
		return
	}

	for i, task := range p.tasks {
		task.State = TaskStateStopping
		if i == toDelete - 1 {
			break
		}
	}
}

// send updates to process
func (p *Process) PushUpdates() error {
	p.logger.DebugWith("Push updates to processor","processor",p.Name, "state", p.AsString())
	if p.IP == "" {
		return nil
	}

	message := p.GetProcessState()
	body, err := json.Marshal(message)
	if err !=nil {
		return errors.Wrap(err, "Failed to Marshal process for update")
	}

	host:= fmt.Sprintf("%s:%d", p.IP, p.Port)
	request := client.ChanRequest{
		Method: "POST",
		HostURL: host,
		Url: fmt.Sprintf("http://%s/events/%s", host, p.job.Name), //TODO: have proper URL
		Body: body,
		NeedResp: false,
		ReturnChan: p.ctx.ProcRespChannel,
	}

	p.ctx.Client.Submit(&request)

	return nil
}

// handle update requests from process or responses following Push Update ops
func (p *Process) HandleUpdates(msg *ProcessMessage, isRequest bool) error {

	p.LastUpdate = time.Now()
	tasksDeleted := false

	// Update state of currently allocated tasks
	for _, ctask := range msg.Tasks {
		taskID := ctask.Id
		if taskID >= p.job.TotalTasks {
			// TODO: need to be in a log, not fail processing
			return fmt.Errorf("illegal TaskID %d is greater than total %d",taskID,p.job.TotalTasks)
		}

		jtask := p.job.GetTask(taskID)
		// TODO: verify the reporting process is the true owner of that task, we may have already re-alocated it
		jtask.LastUpdate = time.Now()
		jtask.CheckPoint = ctask.CheckPoint
		jtask.Progress = ctask.Progress
		jtask.Delay = ctask.Delay

		switch ctask.State {
		case TaskStateDeleted:
			jtask.State = TaskStateUnassigned
			p.RemoveTask(taskID)
			jtask.SetProcess(nil)
			tasksDeleted = true
		// TODO: find which process need to get more tasks and push an update
		case TaskStateCompleted:
			if jtask.State != TaskStateCompleted {
				// if this is the first time we get completion we add the task to completed list
				p.job.CompletedTasks = append(p.job.CompletedTasks, taskID)
			}
			jtask.State = ctask.State
			p.RemoveTask(taskID)
			jtask.SetProcess(nil)
		case TaskStateRunning:
			// verify its a legal transition (e.g. we didnt ask to stop and got an old update)
			if jtask.State == TaskStateRunning || jtask.State == TaskStateAlloc {
				jtask.State = ctask.State
			}
		default:
			// TODO: need to be in a log, not fail processing
			return fmt.Errorf("illegal returned state in task ID %d, %s",taskID, ctask.State)
		}

	}



	// if it is a request from the process check if need to allocate tasks (will respond with updated task list)
	if isRequest && !p.removingJob {
		err := p.job.AllocateTasks(p)
		if err !=nil {
			return errors.Wrap(err, "Failed to allocate tasks")
		}
	}

	// if some tasks deleted (returned to pool) rebalance
	if tasksDeleted {
		p.job.Rebalance()    //TODO: verify no circular dep
	}

	// if in a state of removing job and all tasks removed clear job assosiation
	if p.removingJob && len(p.tasks) == 0 {
		delete(p.job.Processes, p.Name)
		p.job = nil
	}

	return nil

	// TODO: Save current state (checkpoints, completed list ..), or this can be done by the function processor?

}

// return an enriched process struct for API
func (p *Process) GetProcessState() *ProcessMessage  {
	tasklist := []Task{}
	for _, task := range p.tasks {
		tasklist = append(tasklist, Task{Id:task.Id, State:task.State})
	}

	msg := ProcessMessage{}
	msg.Name = p.Name
	msg.Namespace = p.Namespace
	msg.Function = p.Function
	msg.Tasks = tasklist
	if p.job != nil {
		msg.TotalTasks = p.job.TotalTasks
		msg.Metadata = p.job.Metadata
	}
	return &msg
}


// emulate a process, may be broken
func (p *Process) emulateProcess()  {
	tasklist := []Task{}
	for _, task := range p.tasks {
		switch task.State {
		case TaskStateStopping:
			tasklist = append(tasklist, Task{Id:task.Id, State:TaskStateDeleted})
		default:
			tasklist = append(tasklist, Task{Id:task.Id, State:TaskStateRunning})
		}
	}

	msg := ProcessMessage{}
	msg.Tasks = tasklist
	p.HandleUpdates(&msg, true)
}

