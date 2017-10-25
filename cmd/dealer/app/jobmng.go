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

package app

import (
	"fmt"
	"github.com/nuclio/nuclio-sdk"
	"github.com/pkg/errors"

	"github.com/nuclio/nuclio/pkg/dealer/client"
	"github.com/nuclio/nuclio/pkg/dealer/jobs"
)

func NewJobManager(config string, logger nuclio.Logger) (*JobManager, error) {
	newManager := JobManager{}
	newManager.Processes = make(map[string]*jobs.Process)
	newManager.verbose = true   // TODO: from config

	var err error
	newManager.asyncClient, err = client.NewAsyncClient(logger)
	if err != nil {
		return &newManager, err
	}

	procRespChannel := make(chan *client.Response, 100)
	newManager.Ctx = jobs.ManagerContext{ ProcRespChannel: procRespChannel,
		Client:newManager.asyncClient, //RequestsChannel:reqChan,
	}

	// TODO: fixme
	reqChan := make(chan *jobs.RequestMessage, 100)
	newManager.RequestsChannel = reqChan

	reqChan2 := make(chan *jobs.RequestMessage, 100)
	newManager.Ctx.RequestsChannel = reqChan2
	newManager.DeployMap, _ = jobs.NewDeploymentMap(logger, &newManager.Ctx)

	newManager.logger = logger
	return &newManager, nil
}

type JobManager struct {
	logger        nuclio.Logger
	Ctx           jobs.ManagerContext
	RequestsChannel  chan *jobs.RequestMessage
	verbose       bool
	Processes     map[string]*jobs.Process
	DeployMap     *jobs.DeploymentMap
	asyncClient   *client.AsyncClient

}

func (jm *JobManager) SubmitReq(request *jobs.RequestMessage) (interface{}, error) {
	respChan := make(chan *jobs.RespChanType)
	request.ReturnChan = respChan
	jm.Ctx.RequestsChannel <- request
	resp := <- respChan
	return resp.Object, resp.Err
	return nil, nil
}

func (jm *JobManager) Start() error {

	// TODO: need a go routine that verify periodically PODs are up (check last update time and just verify old ones)

	err := jm.asyncClient.Start()
	if err != nil {
		return errors.Wrap(err, "Failed to start job manager - async client")
	}

	go func() {
		for {
			select {
			case resp, ok := <-jm.Ctx.ProcRespChannel:
			// TODO: process responses
				if !ok { break }
				jm.logger.DebugWith("Got proc response", "body", string(resp.Body()))

			case req := <-jm.Ctx.RequestsChannel:
				jm.logger.DebugWith("Got chan request", "type", req.Type, "name", req.Name, "namespace", req.Namespace)
				switch req.Type {
				case jobs.RequestTypeJobGet:
					job, err := jm.GetJob(req.Namespace, req.Function, req.Name)
					req.ReturnChan <- &jobs.RespChanType{Err: err, Object: job}

				case jobs.RequestTypeJobCreate:
					job := req.Object.(*jobs.Job)
					err := jm.AddJob(job)
					req.ReturnChan <- &jobs.RespChanType{Err: err, Object: job}

				case jobs.RequestTypeJobDel:
					err := jm.RemoveJob(req.Name, req.Namespace)
					req.ReturnChan <- &jobs.RespChanType{Err: err}

				case jobs.RequestTypeJobList:
					list := jm.DeployMap.ListJobs(req.Namespace,req.Function, "")
					req.ReturnChan <- &jobs.RespChanType{Err: nil, Object: list}

				case jobs.RequestTypeJobUpdate:
					// TODO: consider what need to allow in update
					job, err := jm.GetJob(req.Namespace, req.Function, req.Name)
					if err != nil {
						req.ReturnChan <- &jobs.RespChanType{ Err: err, Object: job}
					} else {
						err := jm.UpdateJob(job, req.Object.(*jobs.JobMessage))
						req.ReturnChan <- &jobs.RespChanType{Err: err, Object: job}
					}


				case jobs.RequestTypeProcGet:
					proc, ok := jm.Processes[jobs.ProcessKey(req.Name,req.Namespace)]
					if !ok {
						req.ReturnChan <- &jobs.RespChanType{
							Err: fmt.Errorf("Process %s not found", req.Name),
							Object: proc,
						}
					} else {
						req.ReturnChan <- &jobs.RespChanType{
							Err: nil, Object: proc.GetProcessState()}
					}

				case jobs.RequestTypeProcCreate:
					baseproc := req.Object.(*jobs.BaseProcess)
					proc := &jobs.Process{BaseProcess: *baseproc}
					err := jm.AddProcess(proc)
					req.ReturnChan <- &jobs.RespChanType{
						Err: err, Object: proc.GetProcessState()}

				case jobs.RequestTypeProcDel:
					err := jm.RemoveProcess(req.Name, req.Namespace)
					req.ReturnChan <- &jobs.RespChanType{Err: err}

				case jobs.RequestTypeProcList:
					list := []*jobs.ProcessMessage{}
					for _, p := range jm.Processes {
						if req.Namespace == "" || req.Namespace == p.Namespace {
							list = append(list, p.GetProcessState())
						}
					}
					req.ReturnChan <- &jobs.RespChanType{Err: nil, Object: list}

				case jobs.RequestTypeProcUpdate:
					proc, ok := jm.Processes[jobs.ProcessKey(req.Name,req.Namespace)]
					if !ok {
						req.ReturnChan <- &jobs.RespChanType{
							Err: fmt.Errorf("Process %s not found", req.Name),
							Object: proc,
						}
					} else {
						err := jm.UpdateProcess(proc, req.Object.(*jobs.ProcessMessage))
						req.ReturnChan <- &jobs.RespChanType{
							Err: err, Object: proc.GetProcessState()}
					}


				case jobs.RequestTypeDeployUpdate:
					dep := req.Object.(*jobs.Deployment)
					err := jm.DeployMap.UpdateDeployment(dep)
					req.ReturnChan <- &jobs.RespChanType{
						Err: err, Object: dep}

				case jobs.RequestTypeDeployList:
					req.ReturnChan <- &jobs.RespChanType{
						Err: nil, Object: jm.DeployMap.GetAllDeployments(req.Namespace, req.Name)}


				}


			}



		}
	}()

	return nil
}


func (jm *JobManager) GetJob(namespace, function, name string) (*jobs.JobMessage, error) {
	list := jm.DeployMap.ListJobs(namespace, function, "")

	for _, job := range list {
		if job.Name == name {
			return job.GetJobState(), nil
		}
	}

	return nil, fmt.Errorf("Job %s %s %s not found", namespace, function, name)
}

// TODO: add job before/after deployment was created
func (jm *JobManager) AddJob(job *jobs.Job) error {

	jm.logger.InfoWith("Adding new job", "job", job)

	job, err := jobs.NewJob(&jm.Ctx, job)
	if err != nil {
		return errors.Wrap(err, "Failed to add job")
	}

	err = jm.DeployMap.JobRequest(job)
	if err != nil {
		return errors.Wrap(err, "Failed to add job to deploymap")
	}

	return nil
}

// TODO: change to dep jobs
func (jm *JobManager) RemoveJob(name, namespace string) error {

	jm.logger.InfoWith("Removing a job", "name", name, "namespace", namespace)

	return nil
}

// TODO: change to update various runtime job params
func (jm *JobManager) UpdateJob(oldJob, newjob *jobs.JobMessage) error {

	jm.logger.InfoWith("Update a job", "old", oldJob, "new", newjob)

	return nil
}


// TODO: diff or merge between POD add/update to process request update
func (jm *JobManager) AddProcess(proc *jobs.Process) error {

	jm.logger.InfoWith("Adding new process", "process", proc)

	proc, err := jobs.NewProcess(jm.logger, &jm.Ctx, proc)
	if err != nil {
		return err
	}

	key := jobs.ProcessKey(proc.Name,proc.Namespace)
	if _, ok := jm.Processes[key]; ok {
		return fmt.Errorf("Process %s already exist", key)
	}

	jm.Processes[key] = proc
	err = jm.DeployMap.UpdateProcess(proc)
	if err != nil {
		return errors.Wrap(err, "Failed to add process to deploymap")
	}

	return nil
}

func (jm *JobManager) RemoveProcess(name, namespace string) error {

	jm.logger.InfoWith("Removing a process", "name", name, "namespace", namespace)

	proc, ok := jm.Processes[jobs.ProcessKey(name, namespace)]
	if !ok {
		return fmt.Errorf("Process %s not found", name)
	}

	// TODO: use DeploymentMap.RemoveProcess
	err := proc.Remove()
	if err != nil {
		return err
	}
	delete(jm.Processes, jobs.ProcessKey(name, namespace))
	return nil
}

func (jm *JobManager) UpdateProcess(oldProc *jobs.Process, newProc *jobs.ProcessMessage) error {

	jm.logger.InfoWith("Update a process", "old", oldProc, "new", newProc)

	return oldProc.HandleUpdates(newProc, true)

}

