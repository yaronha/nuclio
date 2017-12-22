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
	"net/http"
	"time"
)

type Job struct {
	ctx  *ManagerContext
	Name string `json:"name"`
	// Job to function association (namespace, function, version/alias)
	Namespace string `json:"namespace"`
	Function  string `json:"function"`
	Version   string `json:"version,omitempty"`
	// when true the job is suspended
	Suspend bool `json:"suspend,omitempty"`
	// The start time of the job
	StartTime time.Time `json:"startTime,omitempty"`
	// Total number of tasks to be distributed to workers
	TotalTasks int `json:"totalTasks"`
	// Maximum Job tasks executed per processor at a given time
	MaxTaskAllocation int `json:"maxTaskAllocation,omitempty"`
	// the Job was created after the deployment (function) creation, i.e. submitted directly to the dealer
	PostDeployment bool `json:"postDeployment,omitempty"`
	// List of completed tasks
	CompletedTasks []int `json:"completedTasks,omitempty"`
	// Job can spawn multiple versions (e.g. Canary Deployment)
	IsMultiVersion bool `json:"isMultiVersion,omitempty"`
	// Job need to be saved to persistent storage
	markedDirty bool
	// Private Job Metadata, will be passed to the processor as is
	Metadata interface{} `json:"metadata,omitempty"`

	tasks         []*Task
	maxTaskId     int
	assignedTasks int
	IsStopping    bool
}

// Job request and response for the REST API
type JobMessage struct {
	Job
	Tasks []TaskMessage `json:"tasks"`
}

func (j *JobMessage) Bind(r *http.Request) error {
	return nil
}

func (j *JobMessage) Render(w http.ResponseWriter, r *http.Request) error {
	return nil
}

// create a new job, add critical Metadata, and initialize Tasks struct
func NewJob(context *ManagerContext, newJob *Job) (*Job, error) {

	if newJob.Namespace == "" {
		newJob.Namespace = "default"
	}

	newJob.StartTime = time.Now()
	newJob.ctx = context

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
	if num <= 0 || j.IsStopping {
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

func (j *Job) Stop(procs map[string]*Process) error {

	j.IsStopping = true

	for _, proc := range procs {
		err := proc.ClearJobTasks(j.Name)
		if err != nil {
			j.ctx.Logger.ErrorWith("Error when stopping Job - cant clear tasks", "Job", j.Name, "process", proc.Name, "error", err)
		}
	}

	return nil
}
