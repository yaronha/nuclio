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
)

type TaskState int8

const (
	TaskStateUnassigned  TaskState = 0
	TaskStateRunning     TaskState = 1    // process reported it is running
	TaskStateStopping    TaskState = 2    // asking the process to stop/free task
	TaskStateDeleted     TaskState = 3    // process reported is has stopped, we can re-assign
	TaskStateAlloc       TaskState = 4    // Allocated to a process (not acknowledged yet)
	TaskStateCompleted   TaskState = 5    // Task processing completed
)

var StateStrings = map[TaskState]string{TaskStateUnassigned:"-", TaskStateAlloc:"a", TaskStateRunning:" ",
	TaskStateStopping:"s",TaskStateDeleted:"d",TaskStateCompleted:"c"}

var StateNames = map[TaskState]string{TaskStateUnassigned:"Unassigned", TaskStateAlloc:"Alloc", TaskStateRunning:"Running",
	TaskStateStopping:"Stopping",TaskStateDeleted:"Deleted",TaskStateCompleted:"Completed"}

type Task struct {
	Id          int             `json:"id"`
	State       TaskState       `json:"state"`
	CheckPoint  []byte          `json:"checkPoint,omitempty"`
	Progress    int             `json:"progress,omitempty"`
	Delay       int             `json:"delay,omitempty"`
	process     *Process
	LastUpdate  time.Time       `json:"lastUpdate,omitempty"`
}

func (t *Task)String() string {
	return fmt.Sprintf("%d%s",t.Id,StateStrings[t.State])
}

func (t *Task)GetProcess() *Process {
	return t.process
}

func (t *Task)SetProcess(proc *Process)  {
	t.process = proc
}


