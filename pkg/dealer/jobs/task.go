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
	"fmt"
	"time"
)

type TaskState int8

const (
	TaskStateUnassigned TaskState = 0
	TaskStateRunning    TaskState = 1 // process reported it is running
	TaskStateStopping   TaskState = 2 // asking the process to stop/free task
	TaskStateDeleted    TaskState = 3 // process reported is has stopped, we can re-assign
	TaskStateAlloc      TaskState = 4 // Allocated to a process (not acknowledged yet)
	TaskStateCompleted  TaskState = 5 // Task processing completed
)

var StateStrings = map[TaskState]string{TaskStateUnassigned: "-", TaskStateAlloc: "a", TaskStateRunning: " ",
	TaskStateStopping: "s", TaskStateDeleted: "d", TaskStateCompleted: "c"}

var StateNames = map[TaskState]string{TaskStateUnassigned: "Unassigned", TaskStateAlloc: "Alloc", TaskStateRunning: "Running",
	TaskStateStopping: "Stopping", TaskStateDeleted: "Deleted", TaskStateCompleted: "Completed"}

type BaseTask struct {
	// Task index within the job
	Id int `json:"id"`
	// Current state of the task
	state TaskState `json:"state"`
	// Optional, Last checkpoint per task, e.g. the last stream pointer
	// Checkpoint is periodically reported by the process and stored with the job state
	// Checkpoint is provided to the process in case of task migration or restart after failure
	// alternatively processes can store/read the checkpoint data directly to/from the job state record
	CheckPoint []byte `json:"checkPoint,omitempty"`
	// Optional, Amount of events processed reported by the process, for progress indication
	Progress int `json:"progress,omitempty"`
	// Optional, Number of events pending/dalayed reported by the process, for reporting & future dynamic load-balancing
	Delay int `json:"delay,omitempty"`
}

// Task request and response for the REST API
type TaskMessage struct {
	BaseTask
	State   TaskState `json:"state"`
	Process string    `json:"process,omitempty"`
}

func (t *TaskMessage) Copy() TaskMessage {
	return TaskMessage{BaseTask: t.BaseTask, Process: t.Process, State: t.State} // Job:t.Job,
}

type Task struct {
	BaseTask
	process    *Process
	job        *Job
	LastUpdate time.Time `json:"lastUpdate,omitempty"`
}

func NewTask(id int, job *Job) *Task {
	return &Task{BaseTask: BaseTask{Id: id}, job: job}
}

func (t *Task) String() string {
	return fmt.Sprintf("%d%s", t.Id, StateStrings[t.state])
}

func (t *Task) ToMessage(withProc bool) TaskMessage {
	pname := ""
	if withProc && t.process != nil {
		pname = t.process.Name
	}
	return TaskMessage{BaseTask: t.BaseTask, Process: pname, State: t.state}
}

func (t *Task) GetProcess() *Process {
	return t.process
}

func (t *Task) GetState() TaskState {
	return t.state
}

func (t *Task) SetState(state TaskState) {
	if state == TaskStateUnassigned || state == TaskStateCompleted {
		if state != t.state {
			t.job.assignedTasks -= 1
		}
	} else {
		if t.state == TaskStateUnassigned {
			t.job.assignedTasks += 1
		}
	}
	t.state = state
}

func (t *Task) SetProcess(proc *Process) {
	t.process = proc
}

func (t *Task) GetJob() *Job {
	return t.job
}

func (t *Task) SetJob(job *Job) {
	t.job = job
}
