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
	LastEvent     time.Time             `json:"lastEvent,omitempty"`
	TotalEvents   int                   `json:"totalEvents,omitempty"`
}

type Process struct {
	BaseProcess
	deployment    *Deployment
	LastUpdate    time.Time             `json:"lastUpdate,omitempty"`

	//BaseProcess
	logger        nuclio.Logger
	ctx           *ManagerContext
	removingTasks bool
	tasks         []*Task
}

// Process request and response for the REST API
type ProcessMessage struct {
	BaseProcess

	Tasks         []TaskMessage          `json:"tasks,omitempty"`
	Jobs          map[string]JobShort    `json:"jobs,omitempty"`
}

type JobShort struct {
	TotalTasks    int                   `json:"totalTasks"`
	Metadata      interface{}           `json:"metadata,omitempty"`
}

func (p *ProcessMessage) Bind(r *http.Request) error {
	return nil
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
	return fmt.Sprintf("%s-%s:%s",p.Name,p.State,p.tasks)
}

// force remove a process: mark its tasks unassigned, remove from job, rebalance (assign the tasks to other procs)
func (p *Process) Remove() error {

	for _, task := range p.tasks {
		task.State = TaskStateUnassigned
		task.SetProcess(nil)
		task.LastUpdate = time.Now()
	}

	p.removingTasks = true
	return nil
}

// Request to stop all process tasks
func (p *Process) ClearTasks() error {
	if len(p.tasks) == 0 {
		return nil
	}
	p.removingTasks = true

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

// return task based on Id and Job name
func (p *Process) GetTask(job string, id int) *Task {
	for _, task := range p.tasks {
		if task.Id == id && task.job.Name == job {
			return task
		}
	}
	return nil
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

// remove specific task from Process
func (p *Process) RemoveTask(job string, id int) {
	for i, task := range p.tasks {
		if task.Id == id && task.job.Name == job {
			p.tasks = append(p.tasks[:i], p.tasks[i+1:]...)
			return
		}
	}
}

// move N Tasks to state Stopping
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

	// if process IP is unknown or unset return
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
		Url: fmt.Sprintf("http://%s/triggers", host), //TODO: have proper URL
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
		job := p.deployment.jobs[ctask.Job]
		if taskID >= job.TotalTasks {
			// TODO: need to be in a log, not fail processing
			return fmt.Errorf("illegal TaskID %d is greater than total %d",taskID,job.TotalTasks)
		}

		jtask := job.tasks[taskID]
		// TODO: verify the reporting process is the true owner of that task, we may have already re-alocated it
		jtask.LastUpdate = time.Now()
		jtask.CheckPoint = ctask.CheckPoint
		jtask.Progress = ctask.Progress
		jtask.Delay = ctask.Delay

		switch ctask.State {
		case TaskStateDeleted:
			jtask.State = TaskStateUnassigned
			p.RemoveTask(ctask.Job, taskID)
			jtask.SetProcess(nil)
			tasksDeleted = true
		// TODO: find which process need to get more tasks and push an update
		case TaskStateCompleted:
			if jtask.State != TaskStateCompleted {
				// if this is the first time we get completion we add the task to completed list
				job.CompletedTasks = append(job.CompletedTasks, taskID)
			}
			jtask.State = ctask.State
			p.RemoveTask(ctask.Job, taskID)
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
	if isRequest && !p.removingTasks {
		err := p.deployment.AllocateTasks(p)
		if err !=nil {
			return errors.Wrap(err, "Failed to allocate tasks")
		}
	}

	// if some tasks deleted (returned to pool) rebalance
	if tasksDeleted {
		p.deployment.Rebalance()    //TODO: verify no circular dep
	}

	// if in a state of removing tasks and all tasks removed clear job assosiation
	return nil

	// TODO: Save current state (checkpoints, completed list ..), or this can be done by the function processor?

}

// return an enriched process struct for API
func (p *Process) GetProcessState() *ProcessMessage  {
	msg := ProcessMessage{BaseProcess: p.BaseProcess}
	msg.Tasks = []TaskMessage{}
	msg.Jobs = map[string]JobShort{}

	for _, task := range p.tasks {
		msg.Tasks = append(msg.Tasks, TaskMessage{BaseTask:task.BaseTask, Job:task.job.Name})
		if _, ok := msg.Jobs[task.job.Name]; !ok {
			msg.Jobs[task.job.Name] = JobShort{TotalTasks:task.job.TotalTasks, Metadata:task.job.Metadata}
		}
	}

	return &msg
}


// emulate a process, may be broken
func (p *Process) emulateProcess()  {
	tasklist := []TaskMessage{}
	for _, task := range p.tasks {
		taskmsg := TaskMessage{BaseTask:task.BaseTask, Job:task.job.Name}
		switch task.State {
		case TaskStateStopping:
			taskmsg.State = TaskStateDeleted
		default:
			taskmsg.State = TaskStateRunning
		}
		tasklist = append(tasklist, taskmsg)
	}

	msg := ProcessMessage{}
	msg.Tasks = tasklist
	p.HandleUpdates(&msg, true)
}

