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
	"github.com/nuclio/nuclio/pkg/dealer/asyncflow"
	"net/http"
	"time"
)

type JobState int8

const (
	JobStateUnknown    JobState = 0
	JobStateRunning    JobState = 1 // distributed to processes
	JobStateStopping   JobState = 2 // asking the processes to stop/free job task
	JobStateSuspended  JobState = 3 // user requested to suspend the job
	JobStateWaitForDep JobState = 4 // Job is waiting for the deployment to start
	JobStateScheduled  JobState = 5 // Job is scheduled for deployment
	JobStateCompleted  JobState = 6 // Job processing completed
)

type BaseJob struct {
	Name string `json:"name"`
	// when true the job is suspended
	Disable bool `json:"disable,omitempty"`
	// Total number of tasks to be distributed to workers
	TotalTasks int `json:"totalTasks"`
	// Maximum Job tasks executed per processor at a given time
	MaxTaskAllocation int `json:"maxTaskAllocation,omitempty"`
	// Job can spawn multiple versions (e.g. Canary Deployment)
	IsMultiVersion bool `json:"isMultiVersion,omitempty"`
	// Private Job Metadata, will be passed to the processor as is
	Metadata interface{} `json:"metadata,omitempty"`

	// TODO: not used for now, maybe for determining Function min/max
	MinProcesses int `json:"minProcesses,omitempty"`
	MaxProcesses int `json:"maxProcesses,omitempty"`
}

type Job struct {
	BaseJob
	ctx *ManagerContext
	// Job to function association (namespace, function, version/alias)
	Namespace string `json:"namespace"`
	Function  string `json:"function"`
	Version   string `json:"version,omitempty"`
	// The start time of the job
	StartTime time.Time `json:"startTime,omitempty"`
	// the Job was created after the deployment (function) creation, i.e. submitted directly to the dealer
	postDeployment bool `json:"postDeployment,omitempty"`
	// List of completed tasks
	CompletedTasks []int `json:"completedTasks,omitempty"`
	// Job need to be saved to persistent storage
	markedDirty bool
	// Job state
	state JobState
	//desiredState JobState

	tasks         []*Task
	maxTaskId     int
	assignedTasks int
	postStop      *asyncflow.AsyncCB
}

// Job request and response for the REST API
type JobMessage struct {
	Job
	//DesiredState JobState
	Tasks []TaskMessage `json:"tasks"`
}

func (j *JobMessage) Bind(r *http.Request) error {
	return nil
}

func (j *JobMessage) Render(w http.ResponseWriter, r *http.Request) error {
	return nil
}

// create a new job, add critical Metadata, and initialize Tasks struct
func NewJob(context *ManagerContext, newJob *Job, postDeployment bool) (*Job, error) {

	if newJob.Namespace == "" {
		newJob.Namespace = "default"
	}

	newJob.StartTime = time.Now()
	newJob.ctx = context
	newJob.postDeployment = postDeployment
	newJob.postStop = asyncflow.NewAsyncCB()
	if newJob.Disable {
		newJob.state = JobStateSuspended
	} else {
		newJob.state = JobStateRunning
	}

	// Initialize an array of tasks based on the TotalTasks value
	newJob.tasks = make([]*Task, newJob.TotalTasks)
	for i := 0; i < newJob.TotalTasks; i++ {
		newJob.tasks[i] = NewTask(i, newJob)
	}

	return newJob, nil
}

func (j *Job) AsString() string {
	return fmt.Sprintf("%s (%d): {Comp: %d} ", j.Name, j.TotalTasks, j.CompletedTasks)
}

func (j *Job) GetTask(id int) *Task {
	return j.tasks[id]
}

func (j *Job) GetState() JobState {
	return j.state
}

func (j *Job) FromDeployment() bool {
	return !j.postDeployment
}

func (j *Job) UpdateState(state JobState) {
	if state == JobStateSuspended && j.state != JobStateSuspended {
		j.postStop.Call(nil, nil)
	}
	j.state = state
}

func (j *Job) InitTask(task *TaskMessage) {
	id := task.Id

	if task.State == TaskStateCompleted {
		j.tasks[id].state = TaskStateCompleted
		return
	}

	if task.CheckPoint != nil || task.Progress != 0 {
		j.tasks[id].CheckPoint = task.CheckPoint
		j.tasks[id].Progress = task.Progress
	}
}

// return Job message with list of job tasks
func (j *Job) GetJobState() *JobMessage {
	jobMessage := JobMessage{Job: *j}
	jobMessage.Tasks = []TaskMessage{}

	for _, task := range j.tasks {
		jobMessage.Tasks = append(jobMessage.Tasks, task.ToMessage(true))
	}
	return &jobMessage
}

// find N tasks which are unallocated starting from index
func (j *Job) findUnallocTask(num int, from *int) []*Task {
	list := []*Task{}
	if num <= 0 || j.GetState() != JobStateRunning {
		return list
	}

	for i := *from; i < j.TotalTasks; i++ {
		if task := j.GetTask(i); task.GetState() == TaskStateUnassigned {
			list = append(list, task)
		}
		*from++
		if len(list) == num {
			break
		}
	}
	return list
}

// Mark the job as dirty (need saving), return true if it was already dirty
func (j *Job) NeedToSave() bool {
	val := j.markedDirty
	j.markedDirty = true
	return val
}
