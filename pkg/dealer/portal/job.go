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
	"fmt"
	"github.com/go-chi/chi"
	"github.com/go-chi/render"
	"github.com/nuclio/nuclio-sdk"
	"github.com/nuclio/nuclio/pkg/dealer/jobs"
	"net/http"
)

func NewJobsPortal(logger nuclio.Logger, managerCtx *jobs.ManagerContext) (*JobsPortal, error) {
	newJobsPortal := JobsPortal{logger: logger, managerContext: managerCtx}
	return &newJobsPortal, nil
}

type JobsPortal struct {
	logger         nuclio.Logger
	managerContext *jobs.ManagerContext
}

func (jp *JobsPortal) getJob(w http.ResponseWriter, r *http.Request) {
	namespace := chi.URLParam(r, "namespace")
	function := chi.URLParam(r, "function")
	jobID := chi.URLParam(r, "jobID")

	job, err := jp.managerContext.SubmitReq(&jobs.RequestMessage{
		Name: jobID, Namespace: namespace, Function: function, Type: jobs.RequestTypeJobGet})

	if err != nil {
		http.Error(w, http.StatusText(422), 422)
		return
	}

	if err := render.Render(w, r, job.(*jobs.JobMessage)); err != nil {
		render.Render(w, r, ErrRender(err))
		return
	}
}

func (jp *JobsPortal) deleteJob(w http.ResponseWriter, r *http.Request) {

	namespace := chi.URLParam(r, "namespace")
	function := chi.URLParam(r, "function")
	jobID := chi.URLParam(r, "jobID")

	_, err := jp.managerContext.SubmitReq(&jobs.RequestMessage{
		Name: jobID, Namespace: namespace, Function: function, Type: jobs.RequestTypeJobDel})

	if err != nil {
		render.Render(w, r, ErrInvalidRequest(err))
		return
	}

	w.Write([]byte(fmt.Sprintf("Deleted job: %s", jobID)))
}

func (jp *JobsPortal) listJobs(w http.ResponseWriter, r *http.Request) {
	namespace := chi.URLParam(r, "namespace")
	function := chi.URLParam(r, "function")
	list := []render.Renderer{}

	jobList, err := jp.managerContext.SubmitReq(&jobs.RequestMessage{Name: "",
		Namespace: namespace, Function: function, Type: jobs.RequestTypeJobList})

	if err != nil {
		render.Render(w, r, ErrInvalidRequest(err))
		return
	}

	for _, j := range jobList.([]*jobs.JobMessage) {
		list = append(list, j)
	}

	if err := render.RenderList(w, r, list); err != nil {
		render.Render(w, r, ErrRender(err))
		return
	}
}

func (jp *JobsPortal) createJob(w http.ResponseWriter, r *http.Request) {
	data := &jobs.JobMessage{}
	if err := render.Bind(r, data); err != nil {
		render.Render(w, r, ErrInvalidRequest(err))
		return
	}

	_, err := jp.managerContext.SubmitReq(&jobs.RequestMessage{
		Object: &data, Type: jobs.RequestTypeJobCreate})

	if err != nil {
		render.Render(w, r, ErrInvalidRequest(err))
		return
	}

	render.Status(r, http.StatusCreated)
	render.Render(w, r, data)
}

func (jp *JobsPortal) updateJob(w http.ResponseWriter, r *http.Request) {

	namespace := chi.URLParam(r, "namespace")
	function := chi.URLParam(r, "function")
	jobID := chi.URLParam(r, "jobID")

	data := &jobs.JobMessage{}
	if err := render.Bind(r, data); err != nil {
		render.Render(w, r, ErrInvalidRequest(err))
		return
	}

	job, err := jp.managerContext.SubmitReq(&jobs.RequestMessage{
		Name: jobID, Namespace: namespace, Function: function, Object: data.Job, Type: jobs.RequestTypeJobUpdate})

	if err != nil {
		render.Render(w, r, ErrInvalidRequest(err))
		return
	}

	if err := render.Render(w, r, &jobs.JobMessage{Job: job.(jobs.Job)}); err != nil {
		render.Render(w, r, ErrRender(err))
		return
	}
}
