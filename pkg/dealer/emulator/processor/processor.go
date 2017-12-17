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
	"fmt"
	"github.com/go-chi/chi"
	"github.com/go-chi/render"
	"github.com/nuclio/nuclio-sdk"
	"github.com/nuclio/nuclio/pkg/dealer/jobs"
	"net/http"
	"time"
)

func NewProcessEmulator(logger nuclio.Logger, proc *jobs.ProcessMessage) (ProcessEmulator, error) {
	newEmulator := ProcessEmulator{logger: logger}
	newEmulator.proc = &LocalProcess{BaseProcess: proc.BaseProcess}
	newEmulator.proc.jobs = map[string]jobs.JobShort{}
	return newEmulator, nil
}

type ProcessEmulator struct {
	logger nuclio.Logger
	port   int
	proc   *LocalProcess
	//jobs       map[string]*jobs.Job
}

type LocalProcess struct {
	jobs.BaseProcess
	LastUpdate time.Time `json:"lastUpdate,omitempty"`
	jobs       map[string]jobs.JobShort
}

// return an enriched process struct for API
func (p *LocalProcess) GetProcessState() *jobs.ProcessMessage {
	msg := jobs.ProcessMessage{BaseProcess: p.BaseProcess}
	msg.Jobs = p.jobs
	return &msg
}

// Start listener
func (p *ProcessEmulator) Start() error {
	r := chi.NewRouter()
	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		procMsg := p.proc.GetProcessState()

		if err := render.Render(w, r, procMsg); err != nil {
			render.Render(w, r, ErrRender(err))
			return
		}
	})

	r.Route("/triggers", func(r chi.Router) {
		r.Post("/", p.eventUpdate)
	})

	fmt.Printf("Proc %s/%s Function: %s:%s - Listening on port: %d\n", p.proc.Namespace, p.proc.Name, p.proc.Function, p.proc.Version, p.proc.Port)
	return http.ListenAndServe(fmt.Sprintf(":%d", p.proc.Port), r)
}

// update Event and respond with current state
func (p *ProcessEmulator) eventUpdate(w http.ResponseWriter, r *http.Request) {
	data := &jobs.ProcessMessage{}

	if err := render.Bind(r, data); err != nil {
		render.Render(w, r, ErrInvalidRequest(err))
		return
	}

	p.logger.InfoWith("Update", "process", data)
	msg, err := p.emulateProcess(data)

	if err != nil {
		render.Render(w, r, ErrInvalidRequest(err))
		return
	}

	p.logger.InfoWith("Send update resp", "process", msg)
	if err := render.Render(w, r, msg); err != nil {
		render.Render(w, r, ErrRender(err))
		return
	}
}

// Process update message
func (p *ProcessEmulator) emulateProcess(msg *jobs.ProcessMessage) (*jobs.ProcessMessage, error) {

	respmsg := jobs.ProcessMessage{BaseProcess: p.proc.BaseProcess}
	respmsg.Jobs = map[string]jobs.JobShort{}

	for jobname, jobmsg := range msg.Jobs {

		// Add new jobs if not found locally
		job, ok := p.proc.jobs[jobname]
		if !ok {
			job = jobs.JobShort{TotalTasks: jobmsg.TotalTasks, Metadata: jobmsg.Metadata}
			p.logger.DebugWith("Added new job", "name", jobname)
		}

		localList := []jobs.TaskMessage{}
		respList := []jobs.TaskMessage{}
		for _, task := range jobmsg.Tasks {
			// Process Tasks: Stop, Update, or Add
			taskMsg := task.Copy()
			switch task.State {
			case jobs.TaskStateStopping:
				taskMsg.State = jobs.TaskStateDeleted
				respList = append(respList, taskMsg)
			default:
				taskMsg.State = jobs.TaskStateRunning
				localList = append(localList, taskMsg)
				respList = append(respList, taskMsg)
			}
		}

		job.Tasks = localList
		p.proc.jobs[jobname] = job
		respmsg.Jobs[jobname] = jobs.JobShort{TotalTasks: jobmsg.TotalTasks, Metadata: jobmsg.Metadata, Tasks: respList}
	}

	return &respmsg, nil
}
