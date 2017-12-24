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

	"encoding/json"
	"github.com/nuclio/nuclio/pkg/dealer/client"
	"github.com/nuclio/nuclio/pkg/dealer/jobs"
	"io/ioutil"
	"net/http"
	"time"
)

const NUM_WEB_CLIENTS = 4

func NewJobManager(logger nuclio.Logger, config *jobs.ManagerContextConfig) (*JobManager, error) {
	newManager := JobManager{}
	newManager.Processes = make(map[string]*jobs.Process)
	newManager.verbose = true // TODO: from config

	var err error
	newManager.asyncClient, err = client.NewAsyncClient(logger)
	if err != nil {
		return &newManager, err
	}

	newManager.Ctx = jobs.NewManagerContext(logger, newManager.asyncClient, config)
	newManager.DeployMap, _ = jobs.NewDeploymentMap(logger, newManager.Ctx)

	return &newManager, nil
}

type JobManager struct {
	//logger          nuclio.Logger
	Ctx             *jobs.ManagerContext
	RequestsChannel chan *jobs.RequestMessage
	verbose         bool
	Processes       map[string]*jobs.Process
	DeployMap       *jobs.DeploymentMap
	asyncClient     *client.AsyncClient
}

func (jm *JobManager) Start() error {

	// TODO: need a go routine that verify periodically PODs are up (check last update time and just verify old ones)
	// can also use POD watch (periodic) & POD ready hook

	err := jm.asyncClient.Start(NUM_WEB_CLIENTS)
	if err != nil {
		return errors.Wrap(err, "Failed to start job manager - async client")
	}

	go func() {
		for {
			select {
			case resp, ok := <-jm.Ctx.ProcRespChannel:
				// process HTTP resp (from Process updates)
				if !ok {
					break
				}
				jm.Ctx.Logger.DebugWith("Got proc response", "body", string(resp.Body()))
				procMsg := &jobs.ProcessMessage{}
				err := json.Unmarshal(resp.Body(), procMsg)
				if err != nil {
					jm.Ctx.Logger.ErrorWith("Failed to Unmarshal process resp", "body", string(resp.Body()), "err", err)
				}

				// if we get a resp with unspecified proc state, assume it is ready
				if procMsg.State == jobs.ProcessStateUnknown {
					procMsg.State = jobs.ProcessStateReady
				}

				// Update the process state and tasks
				_, err = jm.updateProcess(procMsg, true, false)
				if err != nil {
					jm.Ctx.Logger.ErrorWith("Failed to update process resp", "body", string(resp.Body()), "err", err)
				}

			case asyncTask := <-jm.Ctx.AsyncTasksChannel:
				jm.Ctx.Logger.DebugWith("Got asyncTask", "name", asyncTask.Name, "timeout", asyncTask.IsTimeout())
				if asyncTask.IsTimeout() {
					jm.Ctx.Logger.WarnWith("Async task timed-out", "name", asyncTask.Name)
					asyncTask.CallOnTimeout()
				} else {
					asyncTask.CallOnComplete()
				}

			case req := <-jm.Ctx.RequestsChannel:
				//jm.Ctx.Logger.DebugWith("Got chan request", "type", req.Type, "name", req.Name, "namespace", req.Namespace)
				switch req.Type {
				case jobs.RequestTypeJobGet:
					job, err := jm.getJob(req.Namespace, req.Function, req.Name)
					req.ReturnChan <- &jobs.RespChanType{Err: err, Object: job}

				case jobs.RequestTypeJobCreate:
					job := req.Object.(*jobs.JobMessage)
					newJob, err := jm.addJob(job)
					req.ReturnChan <- &jobs.RespChanType{Err: err, Object: newJob}

				case jobs.RequestTypeJobDel:
					err := jm.removeJob(req.Namespace, req.Function, req.Name)
					req.ReturnChan <- &jobs.RespChanType{Err: err}

				case jobs.RequestTypeJobList:
					list := jm.listJobs(req.Namespace, req.Function, "")
					req.ReturnChan <- &jobs.RespChanType{Err: nil, Object: list}

				case jobs.RequestTypeJobUpdate:
					job, err := jm.updateJob(req.Object.(*jobs.JobMessage))
					req.ReturnChan <- &jobs.RespChanType{Err: err, Object: job}

				case jobs.RequestTypeProcGet:
					proc, ok := jm.Processes[jobs.ProcessKey(req.Name, req.Namespace)]
					if !ok {
						req.ReturnChan <- &jobs.RespChanType{
							Err:    fmt.Errorf("Process %s not found", req.Name),
							Object: proc,
						}
					} else {
						req.ReturnChan <- &jobs.RespChanType{
							Err: nil, Object: proc.GetProcessState()}
					}

				case jobs.RequestTypeProcUpdate:
					procMsg := req.Object.(*jobs.ProcessMessage)
					// if re get a request with unspecified proc state, assume it is ready
					if procMsg.State == jobs.ProcessStateUnknown {
						procMsg.State = jobs.ProcessStateReady
					}
					proc, err := jm.updateProcess(procMsg, true, true)
					req.ReturnChan <- &jobs.RespChanType{
						Err:    err,
						Object: proc,
					}

				// process POD watch updates to figure out if process is ready
				case jobs.RequestTypeProcUpdateState:
					proc := req.Object.(*jobs.BaseProcess)
					updatedProc, err := jm.updateProcess(&jobs.ProcessMessage{BaseProcess: *proc}, false, true)
					if req.ReturnChan != nil {
						req.ReturnChan <- &jobs.RespChanType{
							Err: err, Object: updatedProc}
					}

				case jobs.RequestTypeProcHealth:
					err := jm.processHealth(req.Name, req.Namespace)
					if req.ReturnChan != nil {
						req.ReturnChan <- &jobs.RespChanType{Err: err}
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
					depSpec := req.Object.(*jobs.DeploymentSpec)
					dep, err := jm.DeployMap.UpdateDeployment(depSpec)
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

				case jobs.RequestTypeDeployRemove:
					dep := req.Object.(*jobs.Deployment)
					err := jm.DeployMap.RemoveDeployment(dep.Namespace, dep.Function, dep.Version)
					if req.ReturnChan != nil {
						req.ReturnChan <- &jobs.RespChanType{Err: err}
					}

				}
			}
		}
	}()

	return nil
}

// return a job matching the namespace, function, and job name (job name is unique per ns/function)
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

// Add a job to an existing function (jobs can also be specified in the function spec)
func (jm *JobManager) addJob(job *jobs.JobMessage) (*jobs.JobMessage, error) {

	jm.Ctx.Logger.InfoWith("Adding new job", "job", job)
	dep := jm.DeployMap.FindDeployment(job.Namespace, job.Function, job.Version, true)
	if dep == nil {
		// TODO: if function exist (and is asleep) wake-up the function, and queue the job waiting for deployment
		return nil, fmt.Errorf("Deployment %s %s %s not found, cannot add a job", job.Namespace, job.Function, job.Version)
	}

	newJob, err := dep.AddJob(&job.Job)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to add job to deployment")
	}

	return newJob.GetJobState(), nil
}

// Add a job to an existing function (jobs can also be specified in the function spec)
func (jm *JobManager) InitJobs(namespace string) error {

	jobList, err := jm.Ctx.JobStore.ListJobs(namespace)
	if err != nil {
		return errors.Wrap(err, "Failed to list jobs")
	}

	jm.Ctx.Logger.InfoWith("Init jobs from storage", "jobs", len(jobList))

	for _, job := range jobList {
		dep := jm.DeployMap.FindDeployment(job.Namespace, job.Function, job.Version, true)
		if dep == nil {
			// TODO: if function exist (and is asleep) wake-up the function, and queue the job waiting for deployment
			jm.Ctx.Logger.WarnWith("Deployment not found, cannot init the job", "ns", job.Namespace,
				"function", job.Function, "ver", job.Version, "job", job.Name)
			continue
		}

		newJob, err := dep.AddJob(&job.Job)
		if err != nil {
			jm.Ctx.Logger.WarnWith("Failed to add job to deployment", "ns", job.Namespace,
				"function", job.Function, "ver", job.Version, "job", job.Name, "err", err)
			continue
		}

		for _, taskDef := range job.Tasks {
			newJob.InitTask(&taskDef)
		}

	}

	return nil
}

func (jm *JobManager) findJob(namespace, function, name string) (*jobs.Job, *jobs.Deployment) {
	deps := jm.DeployMap.GetAllDeployments(namespace, function, "")
	for _, dep := range deps {
		for _, job := range dep.GetJobs() {
			if job.Name == name {
				return job, dep
			}
		}
	}
	return nil, nil
}

// remove a Job manually
func (jm *JobManager) removeJob(namespace, function, name string) error {

	jm.Ctx.Logger.InfoWith("Removing a job", "name", name, "namespace", namespace, "function", function)
	job, dep := jm.findJob(namespace, function, name)
	if job != nil {
		if job.FromDeployment() {
			jm.Ctx.Logger.WarnWith("Cannot remove jobs that originate in function spec",
				"function", function, "job", job.Name)
			return fmt.Errorf("Cannot remove jobs that originate in function spec, update the function instead")
		}

		dep.RemoveJob(job, false)
	} else {
		jm.Ctx.Logger.WarnWith("Removing a job, job not found", "name", name, "namespace", namespace)
	}

	return nil
}

// TODO: change to update various runtime job params
func (jm *JobManager) updateJob(newjob *jobs.JobMessage) (*jobs.JobMessage, error) {

	// TODO: consider what need to allow in update and handle it (currently ignored)
	// e.g. update MaxAllocation, Job to Version assosiation, Metadata, TotalTasks
	jm.Ctx.Logger.InfoWith("Update a job", "job", newjob)
	job, dep := jm.findJob(newjob.Namespace, newjob.Function, newjob.Name)

	if job != nil && !job.FromDeployment() {
		if !job.FromDeployment() || job.GetState() != jobs.JobStateRunning || job.GetState() != jobs.JobStateSuspended {
			jm.Ctx.Logger.WarnWith("cant changed job state, from deployment or inactive state",
				"function", newjob.Function, "job", newjob.Name, "fromdep", job.FromDeployment(), "state", job.GetState())
			return nil, fmt.Errorf("cant changed job state, from deployment or inactive state")
		}
		if newjob.Disable != job.Disable {
			jm.Ctx.Logger.DebugWith("job changed state", "function", newjob.Function, "job", newjob.Name, "disable", newjob.Disable)
			job.Disable = newjob.Disable
			job.NeedToSave()
			jm.Ctx.SaveJobs([]*jobs.Job{job})
			if newjob.Disable {
				dep.SuspendJob(job)
			} else {
				job.UpdateState(jobs.JobStateRunning)
				err := dep.Rebalance()
				if err != nil {
					jm.Ctx.Logger.ErrorWith("Failed to rebalance in updateJobs", "deploy", dep.Name, "err", err)
					return nil, err
				}
			}
		}
		return job.GetJobState(), nil

	} else {
		jm.Ctx.Logger.WarnWith("Job not found, cannot update the job", "ns", newjob.Namespace,
			"function", newjob.Function, "job", newjob.Name)
		return nil, fmt.Errorf("Job not found, cannot update the job")

	}

	return nil, nil
}

// remove process, triggered by k8s POD delete or API calls
func (jm *JobManager) removeProcess(name, namespace string) error {

	jm.Ctx.Logger.DebugWith("Removing a process", "name", name, "namespace", namespace)
	key := jobs.ProcessKey(name, namespace)

	proc, ok := jm.Processes[key]
	if !ok {
		jm.Ctx.Logger.WarnWith("Process not found in removeProcess", "name", name, "namespace", namespace)
		return fmt.Errorf("Process %s not found", name)
	}

	dep := proc.GetDeployment()
	// proc.Deployment can be nil if the deployment was removed before the process
	if dep != nil {
		err := dep.RemoveProcess(proc)
		if err != nil {
			return err
		}
	}
	delete(jm.Processes, key)
	return nil
}

// update process health, triggered by k8s events
func (jm *JobManager) processHealth(name, namespace string) error {

	jm.Ctx.Logger.DebugWith("Got heart beat form process", "name", name, "namespace", namespace)
	key := jobs.ProcessKey(name, namespace)
	proc, ok := jm.Processes[key]
	if !ok {
		jm.Ctx.Logger.ErrorWith("Process not found in processHealth", "name", name, "namespace", namespace)
		return fmt.Errorf("Process %s not found", name)
	}

	proc.LastEvent = time.Now()
	return nil
}

// recover process state and tasks during init flow (read current state from processes)
func (jm *JobManager) InitProcesses(procList []*jobs.BaseProcess) {

	for _, proc := range procList {
		jm.Ctx.Logger.DebugWith("Init Process", "proc", proc)

		procMsg := &jobs.ProcessMessage{BaseProcess: *proc}

		key := jobs.ProcessKey(procMsg.Name, procMsg.Namespace)
		proc, ok := jm.Processes[key]

		if ok || procMsg.State != jobs.ProcessStateReady || procMsg.IP == "" {
			jm.Ctx.Logger.DebugWith("process init, ignore process", "exist", ok, "process", procMsg)
			continue
		}

		jm.Ctx.Logger.InfoWith("Adding new process in init", "process", procMsg)

		dep := jm.DeployMap.FindDeployment(procMsg.Namespace, procMsg.Function, procMsg.Version, false)
		if dep == nil {
			// if no deployment than it is likely a new process (no need to restore)
			jm.Ctx.Logger.DebugWith("Deployment wasnt found in process init",
				"namespace", procMsg.Namespace, "function", procMsg.Function, "version", procMsg.Version)
			continue
		}

		// TODO: do HTTP Gets in Go routines (i.e. split the function & use Chan), err handling
		host := fmt.Sprintf("http://%s:%d", procMsg.IP, jobs.DEFAULT_PORT)
		resp, err := http.Get(host)
		if err != nil {
			jm.Ctx.Logger.ErrorWith("process init, Failed get state", "host", host, "err", err)
			continue
		}

		body, _ := ioutil.ReadAll(resp.Body)
		jm.Ctx.Logger.DebugWith("process init, got data", "host", host, "body", string(body))
		procResp := &jobs.ProcessMessage{}
		err = json.Unmarshal(body, procResp)
		if err != nil {
			jm.Ctx.Logger.ErrorWith("Failed to Unmarshal process resp", "body", string(body), "err", err)
			continue
		}

		proc, err = jobs.NewProcess(jm.Ctx.Logger, jm.Ctx, procResp)
		if err != nil {
			jm.Ctx.Logger.ErrorWith("Failed to init NewProcess", "deploy", dep.Name, "proc", proc.Name, "err", err)
			continue
		}

		jm.Processes[key] = proc
		dep.AddProcess(proc)

		err = proc.HandleTaskUpdates(procResp, false, true)
		if err != nil {
			jm.Ctx.Logger.ErrorWith("Failed to init process, HandleTaskUpdates", "deploy", dep.Name, "proc", proc.Name, "err", err)
		}

	}
}

func (jm *JobManager) RebalanceNewDeps(newDepList []*jobs.Deployment) {

	jm.Ctx.Logger.DebugWith("Rebalance deployments", "deployments", len(newDepList))
	for _, dep := range newDepList {
		err := dep.Rebalance()
		if err != nil {
			jm.Ctx.Logger.ErrorWith("Failed to rebalance new deployment", "deploy", dep.Name, "err", err)
		}
	}

}

// Update process state & tasks, triggered by: k8s updates, api requests, or process responses
func (jm *JobManager) updateProcess(procMsg *jobs.ProcessMessage, checkTasks bool, isRequest bool) (*jobs.ProcessMessage, error) {

	key := jobs.ProcessKey(procMsg.Name, procMsg.Namespace)
	proc, ok := jm.Processes[key]

	if !ok {

		if procMsg.State != jobs.ProcessStateReady {
			jm.Ctx.Logger.DebugWith("process update, new and state is not ready", "process", procMsg)
			return procMsg, nil
		}

		jm.Ctx.Logger.InfoWith("Adding new process", "process", procMsg)

		dep := jm.DeployMap.FindDeployment(procMsg.Namespace, procMsg.Function, procMsg.Version, false)
		if dep == nil {
			// TODO: may have a case where the deployment update is delayed, and need to init a dummy deploy
			jm.Ctx.Logger.ErrorWith("Deployment wasnt found in process update",
				"namespace", procMsg.Namespace, "function", procMsg.Function, "version", procMsg.Version)
			return nil, fmt.Errorf("Failed to add process, deployment %s %s %s not found", procMsg.Namespace, procMsg.Function, procMsg.Version)
		}

		var err error
		proc, err = jobs.NewProcess(jm.Ctx.Logger, jm.Ctx, procMsg)
		if err != nil {
			return nil, err
		}

		jm.Processes[key] = proc
		dep.AddProcess(proc)

		if dep.ExpectedProc >= len(dep.GetProcs()) && procMsg.State == jobs.ProcessStateReady {
			err := dep.AllocateTasks(proc)
			if err != nil {
				jm.Ctx.Logger.ErrorWith("Failed to allocate jobtasks to proc", "deploy", dep.Name, "proc", proc.Name, "err", err)
			}
			return proc.GetProcessState(), nil
		}
	}

	jm.Ctx.Logger.DebugWith("Update a process", "old", proc, "new", procMsg)
	proc.LastUpdate = time.Now()

	// TODO: handle state transitions
	if proc.State != procMsg.State {
		jm.Ctx.Logger.InfoWith("Updated process state", "process", proc.Name, "old", proc.State, "new", procMsg.State)
		proc.State = procMsg.State
	}

	if checkTasks {
		err := proc.HandleTaskUpdates(procMsg, isRequest, false)
		if err != nil {
			return nil, err
		}
	}

	return proc.GetProcessState(), nil
}
