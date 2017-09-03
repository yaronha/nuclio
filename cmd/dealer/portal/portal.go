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
	"github.com/nuclio/nuclio/cmd/dealer/app"
	"fmt"
	"github.com/nuclio/nuclio-sdk"
)

var JobManager *app.JobManager

func NewPortal(logger nuclio.Logger, jobManager *app.JobManager, port int) (DealerPortal, error) {
	newPortal := DealerPortal{jobManager:jobManager, port:port, logger:logger}
	JobManager = jobManager
	return newPortal, nil
}


type DealerPortal struct {
	jobManager *app.JobManager
	logger     nuclio.Logger
	port       int

}


func (d *DealerPortal) Start() error {

	jobsPortal, _ := NewJobsPortal(d.logger, d.jobManager)
	procPortal, _ := NewProcPortal(d.logger, d.jobManager)

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
				r.Use(jobsPortal.JobCtx)
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
				r.Use(procPortal.ProcessCtx)
				r.Get("/", procPortal.getProcess)
				//r.Put("/", procPortal.updateProcess)
				r.Delete("/", procPortal.deleteProcess)
			})
		})
	})

	fmt.Printf("Starting Dealer Portal in port: %d\n", d.port)
	return http.ListenAndServe(fmt.Sprintf(":%d", d.port), r)
}

