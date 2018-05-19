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
	"github.com/nuclio/nuclio/pkg/dealer/jobs"
	"net/http"
	"github.com/nuclio/logger"
)

func NewPortal(log logger.Logger, managerCtx *jobs.ManagerContext, port int) (DealerPortal, error) {
	newPortal := DealerPortal{
		managerContext: managerCtx,
		port:           port,
		logger:         log.GetChild("portal").(logger.Logger)}
	return newPortal, nil
}

type DealerPortal struct {
	managerContext *jobs.ManagerContext
	logger         logger.Logger
	port           int
}

func (d *DealerPortal) Start() error {

	jobsPortal, _ := NewJobsPortal(d.logger, d.managerContext)
	procPortal, _ := NewProcPortal(d.logger, d.managerContext)

	r := chi.NewRouter()
	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("welcome"))
	})

	r.Route("/jobs", func(r chi.Router) {
		r.Get("/", jobsPortal.listJobs)
		r.Post("/", jobsPortal.createJob)
		// Subrouters:

		// TODO: add func level
		r.Route("/{namespace}", func(r chi.Router) {
			r.Get("/", jobsPortal.listJobs)
			r.Route("/{function}", func(r chi.Router) {
				r.Get("/", jobsPortal.listJobs)

				r.Route("/{jobID}", func(r chi.Router) {
					r.Get("/", jobsPortal.getJob)
					r.Put("/", jobsPortal.updateJob)
					r.Delete("/", jobsPortal.deleteJob)
				})
			})
		})
	})

	r.Route("/procs", func(r chi.Router) {
		r.Get("/", procPortal.listProcess)
		r.Post("/", procPortal.updateProcess)
		r.Put("/", procPortal.updateProcessState)
		// Subrouters:

		r.Route("/{namespace}", func(r chi.Router) {
			r.Get("/", procPortal.listProcess)

			r.Route("/{procID}", func(r chi.Router) {
				r.Get("/", procPortal.getProcess)
				r.Delete("/", procPortal.deleteProcess)
			})
		})
	})

	r.Route("/deploy", func(r chi.Router) {
		r.Post("/", d.updateDeployment)
		r.Get("/", d.listDeployments)
		r.Delete("/", d.removeDeployment)

		r.Route("/{namespace}", func(r chi.Router) {
			r.Get("/", d.listDeployments)

			r.Route("/{function}", func(r chi.Router) {
				r.Get("/", d.listDeployments)
			})
		})

	})

	fmt.Printf("Starting Dealer Portal in port: %d\n", d.port)
	return http.ListenAndServe(fmt.Sprintf(":%d", d.port), r)
}

func (d *DealerPortal) updateDeployment(w http.ResponseWriter, r *http.Request) {
	data := &DeployRequest{}
	if err := render.Bind(r, data); err != nil {
		render.Render(w, r, ErrInvalidRequest(err))
		return
	}

	dep, err := d.managerContext.SubmitReq(&jobs.RequestMessage{
		Object: data.Deployment, Type: jobs.RequestTypeDeployUpdate})

	if err != nil {
		render.Render(w, r, ErrInvalidRequest(err))
		return
	}

	if err := render.Render(w, r, dep.(*jobs.DeploymentMessage)); err != nil {
		render.Render(w, r, ErrRender(err))
		return
	}
}

func (d *DealerPortal) removeDeployment(w http.ResponseWriter, r *http.Request) {
	data := &DeployRequest{}
	if err := render.Bind(r, data); err != nil {
		render.Render(w, r, ErrInvalidRequest(err))
		return
	}

	_, err := d.managerContext.SubmitReq(&jobs.RequestMessage{
		Object: data.Deployment, Type: jobs.RequestTypeDeployRemove})

	if err != nil {
		render.Render(w, r, ErrInvalidRequest(err))
		return
	}

	w.Write([]byte(fmt.Sprintf("Deleted deployment: %s", data.Deployment.Name)))
}

func (jp *DealerPortal) listDeployments(w http.ResponseWriter, r *http.Request) {
	namespace := chi.URLParam(r, "namespace")
	function := chi.URLParam(r, "function")
	list := []render.Renderer{}

	depList, err := jp.managerContext.SubmitReq(&jobs.RequestMessage{
		Namespace: namespace, Function: function, Type: jobs.RequestTypeDeployList})

	if err != nil {
		render.Render(w, r, ErrInvalidRequest(err))
		return
	}

	for _, dep := range depList.([]*jobs.DeploymentMessage) {
		list = append(list, dep)
	}

	if err := render.RenderList(w, r, list); err != nil {
		render.Render(w, r, ErrRender(err))
		return
	}
}

type DeployRequest struct {
	*jobs.Deployment
}

func (d *DeployRequest) Bind(r *http.Request) error {
	return nil
}

func (d *DeployRequest) Render(w http.ResponseWriter, r *http.Request) error {
	return nil
}
