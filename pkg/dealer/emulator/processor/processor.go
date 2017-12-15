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

package processor

import (
	"github.com/go-chi/render"
	"github.com/nuclio/nuclio-sdk"
	"github.com/go-chi/chi"
	"fmt"
	"net/http"
	"github.com/nuclio/nuclio/pkg/dealer/jobs"
	"os"
)

func NewProcessEmulator(logger nuclio.Logger, proc *jobs.Process) (ProcessEmulator, error) {
	newEmulator := ProcessEmulator{logger:logger}
	newproc, err := jobs.NewProcess(logger, nil, &jobs.ProcessMessage{BaseProcess: proc.BaseProcess})
	if err != nil {
		fmt.Printf("Failed to create process: %s", err)
		os.Exit(1)
	}
	newEmulator.proc = newproc
	newEmulator.jobs = map[string]*jobs.Job{}
	return newEmulator, nil
}

type ProcessEmulator struct {
	logger     nuclio.Logger
	port       int
	proc       *jobs.Process
	jobs       map[string]*jobs.Job
}

// Start listener
func (p *ProcessEmulator) Start() error {
	r := chi.NewRouter()
	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		procMsg := jobs.ProcessMessage{BaseProcess: p.proc.BaseProcess}
		procMsg.Tasks = []jobs.TaskMessage{}
		procMsg.Jobs = map[string]jobs.JobShort{}

		for _, task := range p.proc.GetTasks(false) {
			taskJob := task.GetJob()
			procMsg.Tasks = append(procMsg.Tasks, jobs.TaskMessage{BaseTask:task.BaseTask, Job:taskJob.Name})
			if _, ok := procMsg.Jobs[taskJob.Name]; !ok {
				procMsg.Jobs[taskJob.Name] = jobs.JobShort{TotalTasks:taskJob.TotalTasks, Metadata:taskJob.Metadata}
			}
		}

		if err := render.Render(w, r, &procMsg); err != nil {
			render.Render(w, r, ErrRender(err))
			return
		}
	})

	r.Route("/events", func(r chi.Router) {
		r.Post("/", p.eventUpdate)
	})


	fmt.Printf("Proc %s/%s Function: %s:%s - Listening on port: %d\n",p.proc.Namespace ,p.proc.Name , p.proc.Function, p.proc.Version, p.proc.Port)
	return http.ListenAndServe(fmt.Sprintf(":%d", p.proc.Port), r)
}

// update Event and respond with current state
func (p *ProcessEmulator) eventUpdate(w http.ResponseWriter, r *http.Request) {
	data := &jobs.ProcessMessage{}

	if err := render.Bind(r, data); err != nil {
		render.Render(w, r, ErrInvalidRequest(err))
		return
	}

	p.logger.InfoWith("Update","process",data)
	msg, err := p.emulateProcess(data)

	if err != nil {
		render.Render(w, r, ErrInvalidRequest(err))
		return
	}

	p.logger.InfoWith("Send update resp","process",msg)
	if err := render.Render(w, r, msg); err != nil {
		render.Render(w, r, ErrRender(err))
		return
	}
}
// Process update message
func (p *ProcessEmulator) emulateProcess(proc *jobs.ProcessMessage)  (*jobs.ProcessMessage, error)  {

	for jobname, jobmeta := range proc.Jobs {

		// Add new jobs if not found locally
		_, ok := p.jobs[jobname]
		if !ok {
			newJob := &jobs.Job{Name:jobname, Namespace:p.proc.Namespace,
				Function:p.proc.Function, Version:p.proc.Version,
				TotalTasks:jobmeta.TotalTasks,
			}
			p.jobs[jobname] = newJob
		}

	}

	tasklist := []jobs.TaskMessage{}
	for _, task := range proc.Tasks {
		// Process Tasks: Stop, Update, or Add
		taskMsg := task.Copy()
		switch task.State {
		case jobs.TaskStateStopping:
			taskMsg.State = jobs.TaskStateDeleted
			tasklist = append(tasklist, taskMsg)
			p.proc.RemoveTask(task.Job, task.Id)
		default:
			localTask := p.proc.GetTask(task.Job, task.Id)
			if localTask == nil {
				job := p.jobs[task.Job]
				localTask = &jobs.Task{BaseTask:jobs.BaseTask{Id:task.Id}}
				localTask.SetJob(job)
				p.proc.AddTasks([]*jobs.Task{localTask})
			}
			localTask.State = jobs.TaskStateRunning
			taskMsg.State = jobs.TaskStateRunning
			tasklist = append(tasklist, taskMsg)
		}
	}

	proc.Tasks = tasklist
	return proc, nil
}

