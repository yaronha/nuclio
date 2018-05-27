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
	"github.com/nuclio/nuclio/pkg/dealer/jobs"
	"net/http"
	"time"
	"github.com/nuclio/logger"
)

func NewProcessEmulator(logger logger.Logger, proc *jobs.ProcessMessage) (ProcessEmulator, error) {
	newEmulator := ProcessEmulator{logger: logger}
	newEmulator.Proc = NewLocalProcess(logger, proc)
	return newEmulator, nil
}

type ProcessEmulator struct {
	logger logger.Logger
	port   int
	Proc   *LocalProcess
}

// Start listener
func (p *ProcessEmulator) Start() error {
	r := chi.NewRouter()
	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		procMsg := p.Proc.GetProcessState()
		p.logger.DebugWith("Get proc", "proc", procMsg)

		if err := render.Render(w, r, procMsg); err != nil {
			render.Render(w, r, ErrRender(err))
			return
		}
	})

	r.Route("/triggers", func(r chi.Router) {
		r.Post("/", p.eventUpdate)
	})

	fmt.Printf("Proc %s/%s Function: %s:%s - Listening on port: %d\n", p.Proc.Namespace, p.Proc.Name,
		p.Proc.Function, p.Proc.Version, p.Proc.Port)
	return http.ListenAndServe(fmt.Sprintf(":%d", p.Proc.Port), r)
}

// update Event and respond with current state
func (p *ProcessEmulator) eventUpdate(w http.ResponseWriter, r *http.Request) {
	data := &jobs.ProcessMessage{}

	if err := render.Bind(r, data); err != nil {
		render.Render(w, r, ErrInvalidRequest(err))
		return
	}

	p.logger.InfoWith("Update", "process", data)
	msg, err := p.Proc.ProcessUpdates(data)

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


func NewLocalProcess(logger logger.Logger, proc *jobs.ProcessMessage) *LocalProcess {
	localProc := &LocalProcess{BaseProcess: proc.BaseProcess}
	localProc.jobs = map[string]jobs.JobShort{}
	localProc.State = jobs.ProcessStateReady
	return localProc
}

type LocalProcess struct {
	jobs.BaseProcess
	logger      logger.Logger
	LastUpdate  time.Time
	jobs        map[string]jobs.JobShort
}

// return an enriched process struct for API
func (p *LocalProcess) GetProcessState() *jobs.ProcessMessage {
	msg := jobs.ProcessMessage{BaseProcess: p.BaseProcess}
	msg.Jobs = p.jobs
	return &msg
}


// Process update message
func (p *LocalProcess) ProcessUpdates(msg *jobs.ProcessMessage) (*jobs.ProcessMessage, error) {

	respmsg := jobs.ProcessMessage{BaseProcess: p.BaseProcess}
	respmsg.Jobs = map[string]jobs.JobShort{}

	for jobname, jobmsg := range msg.Jobs {

		// Add new jobs if not found locally
		job, ok := p.jobs[jobname]
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
		p.jobs[jobname] = job
		respmsg.Jobs[jobname] = jobs.JobShort{TotalTasks: jobmsg.TotalTasks, Metadata: jobmsg.Metadata, Tasks: respList}
	}

	return &respmsg, nil
}
