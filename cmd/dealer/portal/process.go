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

package portal

import (
	"github.com/nuclio/nuclio/pkg/dealer/jobs"
	"github.com/go-chi/render"
	"fmt"
	"github.com/go-chi/chi"
	"net/http"
	"context"
	"github.com/nuclio/nuclio-sdk"
	"github.com/nuclio/nuclio/cmd/dealer/app"
)

func NewProcPortal(logger nuclio.Logger, manager *app.JobManager) (*ProcPortal, error) {
	newProcPortal := ProcPortal{logger:logger, jobManager:manager}
	return &newProcPortal, nil
}

type ProcPortal struct {
	logger nuclio.Logger
	jobManager *app.JobManager
}

func (pp *ProcPortal) ProcessCtx(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Println("PC",r)
		namespace := chi.URLParam(r, "namespace")
		procID := chi.URLParam(r, "procID")
		proc, ok := JobManager.Processes[jobs.ProcessKey(procID,namespace)]
		if !ok {
			http.Error(w, http.StatusText(404), 404)
			return
		}
		ctx := context.WithValue(r.Context(), "proc", proc)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (pp *ProcPortal) getProcess(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	proc, ok := ctx.Value("proc").(*jobs.Process)
	if !ok {
		http.Error(w, http.StatusText(422), 422)
		return
	}
	if err := render.Render(w, r, &ProcessRequest{Process:proc}); err != nil {
		render.Render(w, r, ErrRender(err))
		return
	}
}

func (pp *ProcPortal) deleteProcess(w http.ResponseWriter, r *http.Request) {
	proc := r.Context().Value("proc").(*jobs.Process)
	err := JobManager.RemoveProcess(proc.Name, proc.Namespace)
	if err != nil {
		render.Render(w, r, ErrInvalidRequest(err))
		return
	}

	w.Write([]byte(fmt.Sprintf("Deleted process: %s",proc.Name)))
}

func (pp *ProcPortal) listProcess(w http.ResponseWriter, r *http.Request) {
	namespace := chi.URLParam(r, "namespace")
	list := []render.Renderer{}
	for _, p := range JobManager.Processes {
		if namespace == "" || namespace == p.Namespace {
			list = append(list, &ProcessRequest{Process:p})
		}
	}
	if err := render.RenderList(w, r, list ); err != nil {
		render.Render(w, r, ErrRender(err))
		return
	}

}

func (pp *ProcPortal) createProcess(w http.ResponseWriter, r *http.Request) {
	data := &ProcessRequest{}
	if err := render.Bind(r, data); err != nil {
		render.Render(w, r, ErrInvalidRequest(err))
		return
	}

	JobManager.AddProcess(data.Process)

	render.Status(r, http.StatusCreated)
	render.Render(w, r, data)
}


type ProcessRequest struct {
	*jobs.Process
	Tasks []jobs.Task
}

func (p *ProcessRequest) Bind(r *http.Request) error {
	return nil
}

func (p *ProcessRequest) Render(w http.ResponseWriter, r *http.Request) error {
	p.Tasks = []jobs.Task{}
	for _, task := range p.Process.GetTasks(false) {
		p.Tasks = append(p.Tasks, jobs.Task{Id:task.Id, State:task.State})
	}
	return nil
}




