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
	"net/http"
	"github.com/go-chi/chi"
	"fmt"
	"github.com/nuclio/nuclio-sdk"
	"github.com/nuclio/nuclio/pkg/dealer/jobs"
)

func NewPortal(logger nuclio.Logger, managerCtx *jobs.ManagerContext, port int) (DealerPortal, error) {
	newPortal := DealerPortal{
		managerContext:managerCtx ,
		port:port,
		logger: logger.GetChild("portal").(nuclio.Logger)}
	return newPortal, nil
}


type DealerPortal struct {
	managerContext *jobs.ManagerContext
	logger     nuclio.Logger
	port       int

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

		r.Route("/{namespace}", func(r chi.Router) {
			r.Get("/", jobsPortal.listJobs)
			r.Route("/{jobID}", func(r chi.Router) {
				//r.Use(jobsPortal.JobCtx)
				r.Get("/", jobsPortal.getJob)
				r.Put("/", jobsPortal.updateJob)
				r.Delete("/", jobsPortal.deleteJob)
			})
		})
	})

	r.Route("/procs", func(r chi.Router) {
		r.Get("/", procPortal.listProcess)
		r.Post("/", procPortal.createProcess)
		// Subrouters:

		r.Route("/{namespace}", func(r chi.Router) {
			r.Get("/", procPortal.listProcess)
			r.Route("/{procID}", func(r chi.Router) {
				r.Get("/", procPortal.getProcess)
				r.Put("/", procPortal.updateProcess)
				r.Delete("/", procPortal.deleteProcess)
			})
		})
	})

	fmt.Printf("Starting Dealer Portal in port: %d\n", d.port)
	return http.ListenAndServe(fmt.Sprintf(":%d", d.port), r)
}

