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
	newManager.Ctx = &jobs.ManagerContext{ ProcRespChannel: procRespChannel,
		Client:newManager.asyncClient, //RequestsChannel:reqChan,
	}

	// TODO: fixme
	reqChan := make(chan *jobs.RequestMessage, 100)
	newManager.RequestsChannel = reqChan

	reqChan2 := make(chan *jobs.RequestMessage, 100)
	newManager.Ctx.RequestsChannel = reqChan2
	newManager.DeployMap, _ = jobs.NewDeploymentMap(logger, newManager.Ctx)

	newManager.logger = logger
	return &newManager, nil
}

type JobManager struct {
	logger        nuclio.Logger
	Ctx           *jobs.ManagerContext
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
	// can also use POD watch (periodic) & POD ready hook

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
					job, err := jm.getJob(req.Namespace, req.Function, req.Name)
					req.ReturnChan <- &jobs.RespChanType{Err: err, Object: job}

				case jobs.RequestTypeJobCreate:
					job := req.Object.(*jobs.Job)
					newJob, err := jm.addJob(job)
					req.ReturnChan <- &jobs.RespChanType{Err: err, Object: newJob}

				case jobs.RequestTypeJobDel:
					err := jm.removeJob(req.Name, req.Namespace)
					req.ReturnChan <- &jobs.RespChanType{Err: err}

				case jobs.RequestTypeJobList:
					list := jm.listJobs(req.Namespace,req.Function, "")
					req.ReturnChan <- &jobs.RespChanType{Err: nil, Object: list}

				case jobs.RequestTypeJobUpdate:
					// TODO: consider what need to allow in update
					job, err := jm.getJob(req.Namespace, req.Function, req.Name)
					if err != nil {
						req.ReturnChan <- &jobs.RespChanType{ Err: err, Object: job}
					} else {
						err := jm.updateJob(job, req.Object.(*jobs.JobMessage))
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

				case jobs.RequestTypeProcUpdate:
					procMsg := req.Object.(*jobs.ProcessMessage)
					proc, err := jm.updateProcess(procMsg)
					req.ReturnChan <- &jobs.RespChanType{
						Err: err,
						Object: proc,
					}

				// process POD watch updates to figure out if process is ready
				case jobs.RequestTypeProcUpdateState:
					proc := req.Object.(*jobs.BaseProcess)
					err := jm.updateProcessState(proc)
					if req.ReturnChan != nil {
						req.ReturnChan <- &jobs.RespChanType{
							Err: err, Object: nil}
					}

				case jobs.RequestTypeProcDel:
					err := jm.removeProcess(req.Name, req.Namespace)
					if req.ReturnChan != nil {
						req.ReturnChan <- &jobs.RespChanType{Err: err}
					}

				case jobs.RequestTypeProcList:
					list := []*jobs.ProcessMessage{}
					for _, p := range jm.Processes {
						if req.Namespace == "" || req.Namespace == p.Namespace {
							list = append(list, p.GetProcessState())
						}
					}
					req.ReturnChan <- &jobs.RespChanType{Err: nil, Object: list}


				case jobs.RequestTypeDeployUpdate:
					dep := req.Object.(*jobs.Deployment)
					err := jm.DeployMap.UpdateDeployment(dep)
					if req.ReturnChan != nil {
						req.ReturnChan <- &jobs.RespChanType{
							Err: err, Object: dep.GetDeploymentState()}
					}

				case jobs.RequestTypeDeployList:
					depList := []*jobs.DeploymentMessage{}
					deps := jm.DeployMap.GetAllDeployments(req.Namespace, req.Function, "")

					for _, dep := range deps {
						depList = append(depList, dep.GetDeploymentState())
					}
					req.ReturnChan <- &jobs.RespChanType{
						Err: nil, Object: depList}

				}
			}
		}
	}()

	return nil
}


func (jm *JobManager) getJob(namespace, function, name string) (*jobs.JobMessage, error) {
	list := jm.listJobs(namespace, function, "")

	for _, job := range list {
		if job.Name == name {
			return job.GetJobState(), nil
		}
	}

	return nil, fmt.Errorf("Job %s %s %s not found", namespace, function, name)
}

// List jobs assigned to namespace/function, all if no version specified or by version
func (jm *JobManager) listJobs(namespace, function, version string) []*jobs.JobMessage {
	list := []*jobs.JobMessage{}

	deps := jm.DeployMap.GetAllDeployments(namespace, function, version)
	for _, dep := range deps {
		for _, job := range dep.GetJobs() {
			list = append(list, job.GetJobState())
		}
	}

	return list
}

// TODO: add job before/after deployment was created
func (jm *JobManager) addJob(job *jobs.Job) (*jobs.JobMessage, error) {

	jm.logger.InfoWith("Adding new job", "job", job)
	dep := jm.DeployMap.FindDeployment(job.Namespace, job.Function, job.Version, true)
	if dep == nil {
		return nil, fmt.Errorf("Deployment %s %s %s not found, cannot add a job", job.Namespace, job.Function, job.Version)
	}

	newJob, err := jobs.NewJob(jm.Ctx, job)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create a new job")
	}

	err = dep.AddJob(newJob)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to add job to deployment")
	}

	return newJob.GetJobState(), nil
}

// TODO: change to dep jobs
func (jm *JobManager) removeJob(name, namespace string) error {

	jm.logger.InfoWith("Removing a job", "name", name, "namespace", namespace)

	return nil
}

// TODO: change to update various runtime job params
func (jm *JobManager) updateJob(oldJob, newjob *jobs.JobMessage) error {

	jm.logger.InfoWith("Update a job", "old", oldJob, "new", newjob)

	return nil
}


func (jm *JobManager) removeProcess(name, namespace string) error {

	jm.logger.InfoWith("Removing a process", "name", name, "namespace", namespace)
	key := jobs.ProcessKey(name,namespace)

	proc, ok := jm.Processes[key]
	if !ok {
		return fmt.Errorf("Process %s not found", name)
	}

	dep := proc.GetDeployment()
	err := dep.RemoveProcess(proc)
	if err != nil {
		return err
	}
	delete(jm.Processes, key)
	return nil
}

// TODO: updateProcessState
func (jm *JobManager) updateProcessState(proc *jobs.BaseProcess) error {

	return nil
}


func (jm *JobManager) updateProcess(procMsg *jobs.ProcessMessage) (*jobs.ProcessMessage, error) {

	key := jobs.ProcessKey(procMsg.Name,procMsg.Namespace)
	proc, ok := jm.Processes[key]

	if !ok {
		jm.logger.InfoWith("Adding new process", "process", procMsg)

		dep := jm.DeployMap.FindDeployment(procMsg.Namespace, procMsg.Function, procMsg.Version, false)
		if dep == nil {
			// TODO: may have a case where the deployment update is delayed, and need to init a dummy deploy
			jm.logger.ErrorWith("Deployment wasnt found in process update",
				"namespace", procMsg.Namespace, "function", procMsg.Function, "version", procMsg.Version)
			return nil, fmt.Errorf("Failed to add process, deployment %s %s %s not found", procMsg.Namespace, procMsg.Function, procMsg.Version)
		}

		proc, err := jobs.NewProcess(jm.logger, jm.Ctx, procMsg)
		if err != nil {
			return nil, err
		}

		jm.Processes[key] = proc
		dep.AddProcess(proc)

		// TODO: check process state (ready)
		if dep.ExpectedProc > len(dep.GetProcs()) {
			err := dep.AllocateTasks(proc)
			if err != nil {
				jm.logger.ErrorWith("Failed to allocate jobtasks to proc", "deploy", dep.Name, "proc", proc.Name, "err", err)
			}
			return proc.GetProcessState(), nil
		}
	}


	jm.logger.InfoWith("Update a process", "old", proc, "new", procMsg)
	err := proc.HandleUpdates(procMsg, true)
	if err != nil {
		return nil, err
	}

	return proc.GetProcessState(), nil
}


// TODO: update process state (POD updates)