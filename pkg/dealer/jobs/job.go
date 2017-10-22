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

type Job struct {
	ctx           *ManagerContext
	Name               string                `json:"name"`
	Namespace          string                `json:"namespace"`
	FunctionURI        string                `json:"functionURI"`
	Function           string                `json:"function"`
	Version            string                `json:"version,omitempty"`
	Suspend            bool                  `json:"suspend,omitempty"`
	ExpectedProc       int                   `json:"expectedProc"`
	StartTime          time.Time             `json:"startTime,omitempty"`
	TotalTasks         int                   `json:"totalTasks"`
	MaxTaskAllocation  int                   `json:"maxTaskAllocation,omitempty"`
	MinProcesses       int                   `json:"minProcesses,omitempty"`
	MaxProcesses       int                   `json:"maxProcesses,omitempty"`
	//Generation         int
	Processes          map[string]*Process   `json:"processes,omitempty"`
	tasks              []*Task
	fromDeployment     bool
	CompletedTasks     []int                 `json:"completedTasks,omitempty"`
	IsMultiVersion     bool                  `json:"isMultiVersion,omitempty"`
	Metadata           interface{}           `json:"metadata,omitempty"`
}


func NewJob(context *ManagerContext, newJob *Job) (*Job, error) {

	if newJob.Namespace == "" {
		newJob.Namespace = "default"
	}
	if newJob.MinProcesses == 0 {
		newJob.MinProcesses = 1
	}

	newJob.Processes = make(map[string]*Process)
	newJob.tasks = make([]*Task, newJob.TotalTasks)
	newJob.StartTime = time.Now()
	newJob.ctx = context

	for i:=0; i<newJob.TotalTasks; i++ {
		newJob.tasks[i] = &Task{Id:i}
	}
	return newJob, nil
}

func JobKey(name,namespace string) string {return name+"."+namespace}

func (j *Job) AsString() string {
	procs :=""
	for _, p := range j.Processes { procs += p.AsString()+" "}
	return fmt.Sprintf("%s (%d,%d): {Proc: %s, Comp: %d} ",j.Name, j.TotalTasks,j.ExpectedProc,procs, j.CompletedTasks)
}

func (j *Job) GetTask(id int) *Task {
	return j.tasks[id]
}

// return list of job tasks
func (j *Job) GetTasks() []*Task {
	return j.tasks
}

// find N tasks which are unallocated starting from index
func (j *Job) findUnallocTask(num int, from *int) []*Task {
	list := []*Task{}
	if num <= 0 {
		return list
	}

	for i := *from; i < j.TotalTasks; i++  {
		if j.tasks[i].State == TaskStateUnassigned {
			list = append(list, j.tasks[i])
		}
		*from++
		if len(list) == num {
			break
		}
	}
	return list
}

// allocate available tasks to process
func (j *Job) AllocateTasks(proc *Process) error {

	tasksPerProc := j.TotalTasks / j.ExpectedProc
	taskReminder := j.TotalTasks - j.ExpectedProc * tasksPerProc
	var totalAsigned, procTasks, aboveMin int


	for _, p := range j.Processes {
		procTasks = len(p.GetTasks(true))
		totalAsigned += procTasks
		if procTasks > tasksPerProc {
			aboveMin +=1
		}
	}
	//fmt.Println("AllocateTasks")


	totalUnallocated := j.TotalTasks - totalAsigned
	if totalUnallocated < 0 {
		return fmt.Errorf("Assigned Tasks (%d) greater than Total tasks (%d)",totalAsigned,j.TotalTasks)
	}
	if totalUnallocated == 0 {
		return nil
	}

	alloc := tasksPerProc
	if aboveMin < taskReminder {
		alloc += 1
	}
	if j.MaxTaskAllocation > 0 && alloc > j.MaxTaskAllocation {
		alloc = j.MaxTaskAllocation
	}

	newAlloc := alloc - len(proc.GetTasks(true))
	if totalUnallocated <= newAlloc {
		newAlloc = totalUnallocated
	}

	from := 0
	toAlloc := j.findUnallocTask(newAlloc, &from)
	proc.AddTasks(toAlloc)
	return nil
}

// updated expected number of processes and rebalance tasks
func (j *Job) UpdateNumProcesses(newnum int, force bool) error {
	if !force && newnum == j.ExpectedProc {
		return nil
	}
	j.ExpectedProc = newnum

	return j.Rebalance()
}


func (j *Job) Rebalance() error {

	tasksPerProc := (j.TotalTasks - len(j.CompletedTasks)) / j.ExpectedProc
	taskReminder := (j.TotalTasks - len(j.CompletedTasks)) - j.ExpectedProc * tasksPerProc

	var procTasks, missingPlus1, extraPlus1 int
	var tasksUnder, tasksEqual, tasksPlus1, tasksOver []*Process

	for _, p := range j.Processes {
		if !p.removingJob {
			procTasks = len(p.GetTasks(true))
			switch {
			case procTasks < tasksPerProc:
				tasksUnder = append(tasksUnder, p)
			case procTasks == tasksPerProc:
				tasksEqual = append(tasksEqual, p)
			case procTasks == tasksPerProc + 1:
				tasksPlus1 = append(tasksPlus1, p)
			default:
				tasksOver = append(tasksOver, p)
			}
		}
	}

	// desired state is: N with tasksPerProc+1, newnum-N with tasksPerProc (N=taskReminder), not go over MaxAllocation
	// fmt.Println("Tasks (U,E,P1,O):",tasksUnder,tasksEqual,tasksPlus1,tasksOver)

	missingPlus1 = taskReminder - len(tasksPlus1)
	if missingPlus1 < 0 {
		extraPlus1 = -missingPlus1
		missingPlus1 = 0
	}

	for _, p := range tasksOver {
		tasks := p.GetTasks(true)
		if missingPlus1 > 0 {
			p.StopNTasks(len(tasks)-tasksPerProc-1)
			missingPlus1 -= 1
		} else {
			p.StopNTasks(len(tasks)-tasksPerProc)
		}
		_ = p.PushUpdates()
	}

	for _, p := range tasksPlus1 {
		if extraPlus1 > 0 {
			p.StopNTasks(1)
			_ = p.PushUpdates()
			extraPlus1 -= 1
		} else {
			break
		}
	}

	from := 0
	tasksToAdd := append(tasksUnder, tasksEqual...)
	for _, p := range tasksToAdd {
		desired := tasksPerProc
		usedPlus1 := false
		if missingPlus1 > 0 {
			desired += 1
			usedPlus1 = true
		}
		if j.MaxTaskAllocation != 0 && desired > j.MaxTaskAllocation {
			desired = j.MaxTaskAllocation
			usedPlus1 = false
		}

		tasks := j.findUnallocTask(desired - len(p.GetTasks(true)), &from)
		if len(tasks) < desired - len(p.GetTasks(true)) {
			usedPlus1 = false
		}
		p.AddTasks(tasks)
		if usedPlus1 {
			missingPlus1 -= 1
		}
		_ = p.PushUpdates()
	}



	return nil
}
