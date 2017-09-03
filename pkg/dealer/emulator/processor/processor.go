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
	"time"
)

func NewProcessEmulator(logger nuclio.Logger, proc *Process) (ProcessEmulator, error) {
	newEmulator := ProcessEmulator{logger:logger, proc:proc}
	newEmulator.proc.tasks = make(map[int]*jobs.Task)
	return newEmulator, nil
}

type ProcessEmulator struct {
	logger     nuclio.Logger
	port       int
	proc       *Process
}

type Process struct {
	Name          string                `json:"name"`
	Namespace     string                `json:"namespace"`
	Function      string                `json:"function"`
	Version       string                `json:"version,omitempty"`
	Alias         string                `json:"alias,omitempty"`
	IP            string                `json:"ip"`
	Port          int                   `json:"port"`
	Metrics       map[string]int        `json:"metrics,omitempty"`
	State         jobs.ProcessState          `json:"state"`
	LastUpdate    time.Time             `json:"lastUpdate,omitempty"`
	tasks         map[int]*jobs.Task
}

type ProcessUpdate struct {
	*Process
	Tasks         []jobs.Task
}

func (j *ProcessUpdate) Bind(r *http.Request) error {
	return nil
}

func (j *ProcessUpdate) Render(w http.ResponseWriter, r *http.Request) error {
	return nil
}


func (p *ProcessEmulator) Start() error {
	r := chi.NewRouter()
	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		if err := render.Render(w, r, &ProcessUpdate{Process: p.proc}); err != nil {
			render.Render(w, r, ErrRender(err))
			return
		}
	})

	r.Route("/events", func(r chi.Router) {
		r.Route("/{eventName}", func(r chi.Router) {
			r.Post("/", p.update)
		})
	})


	fmt.Printf("Proc %s/%s Function: %s:%s - Listening on port: %d\n",p.proc.Namespace ,p.proc.Name , p.proc.Function, p.proc.Version, p.proc.Port)
	return http.ListenAndServe(fmt.Sprintf(":%d", p.proc.Port), r)
}

func (p *ProcessEmulator) update(w http.ResponseWriter, r *http.Request) {
	data := &ProcessUpdate{}

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

func (p *ProcessEmulator) emulateProcess(proc *ProcessUpdate)  (*ProcessUpdate, error)  {
	tasklist := []jobs.Task{}
	for _, task := range proc.Tasks {
		switch task.State {
		case jobs.TaskStateStopping:
			tasklist = append(tasklist, jobs.Task{Id:task.Id, State:jobs.TaskStateDeleted})
			delete(p.proc.tasks, task.Id)
		default:
			tasklist = append(tasklist, jobs.Task{Id:task.Id, State:jobs.TaskStateRunning})
			p.proc.tasks[task.Id] = &task
		}
	}

	proc.Tasks = tasklist
	return proc, nil
}

