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
	"github.com/go-chi/render"
	"fmt"
	"github.com/go-chi/chi"
	"github.com/nuclio/nuclio/pkg/dealer/jobs"
	"net/http"
	"context"
	"github.com/nuclio/nuclio-sdk"
	"github.com/nuclio/nuclio/cmd/dealer/app"
)

func NewJobsPortal(logger nuclio.Logger, manager *app.JobManager) (*JobsPortal, error) {
	newJobsPortal := JobsPortal{logger:logger, jobManager:manager}
	return &newJobsPortal, nil
}

type JobsPortal struct {
	logger nuclio.Logger
	jobManager *app.JobManager
}



func (jp *JobsPortal) JobCtx(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		namespace := chi.URLParam(r, "namespace")
		jobID := chi.URLParam(r, "jobID")
		job, ok := JobManager.Jobs[jobs.JobKey(jobID,namespace)]
		if !ok {
			http.Error(w, http.StatusText(404), 404)
			return
		}
		ctx := context.WithValue(r.Context(), "job", job)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (jp *JobsPortal) getJob(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	job, ok := ctx.Value("job").(*jobs.Job)
	if !ok {
		http.Error(w, http.StatusText(422), 422)
		return
	}
	if err := render.Render(w, r, &JobRequest{Job:job}); err != nil {
		render.Render(w, r, ErrRender(err))
		return
	}
}

func (jp *JobsPortal) deleteJob(w http.ResponseWriter, r *http.Request) {
	job := r.Context().Value("job").(*jobs.Job)
	err := JobManager.RemoveJob(job.Name, job.Namespace)
	if err != nil {
		render.Render(w, r, ErrInvalidRequest(err))
		return
	}

	w.Write([]byte(fmt.Sprintf("Deleted job: %s",job.Name)))
}

func (jp *JobsPortal) listJobs(w http.ResponseWriter, r *http.Request) {
	namespace := chi.URLParam(r, "namespace")
	list := []render.Renderer{}
	for _, j := range JobManager.Jobs {
		if namespace == "" || namespace == j.Namespace {
			list = append(list, &JobRequest{Job:j})
		}
	}
	if err := render.RenderList(w, r, list ); err != nil {
		render.Render(w, r, ErrRender(err))
		return
	}
}

func (jp *JobsPortal) createJob(w http.ResponseWriter, r *http.Request) {
	data := &JobRequest{}
	if err := render.Bind(r, data); err != nil {
		render.Render(w, r, ErrInvalidRequest(err))
		return
	}

	JobManager.AddJob(data.Job)

	render.Status(r, http.StatusCreated)
	render.Render(w, r, data)
}

func (jp *JobsPortal) updateJob(w http.ResponseWriter, r *http.Request) {
	job := r.Context().Value("job").(*jobs.Job)

	data := &JobRequest{}
	if err := render.Bind(r, data); err != nil {
		render.Render(w, r, ErrInvalidRequest(err))
		return
	}

	err := JobManager.UpdateJob(job, data.Job)
	if err != nil {
		render.Render(w, r, ErrInvalidRequest(err))
		return
	}

	if err := render.Render(w, r, &JobRequest{Job:job}); err != nil {
		render.Render(w, r, ErrRender(err))
		return
	}
}


type JobRequest struct {
	*jobs.Job
	Tasks  []JobTask  `json:"tasks"`

}

type JobTask struct {
	Id      int      `json:"id"`
	State   string   `json:"state"`
	Process string   `json:"process"`
}

func (j *JobRequest) Bind(r *http.Request) error {
	return nil
}

func (j *JobRequest) Render(w http.ResponseWriter, r *http.Request) error {
	j.Tasks = []JobTask{}
	for _, task := range j.Job.GetTasks() {
		pname := ""
		if task.GetProcess() != nil {
			pname = task.GetProcess().Name
		}
		j.Tasks = append(j.Tasks, JobTask{Id:task.Id, State:jobs.StateNames[task.State], Process:pname} )
	}
	return nil
}

