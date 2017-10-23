package jobs

import (
	"github.com/nuclio/nuclio-sdk"
	"fmt"
	"strconv"
	"strings"
)

type Deployment struct {
	Namespace     string                `json:"namespace"`
	Function      string                `json:"function"`
	Name          string                `json:"name"`
	Version       string                `json:"version,omitempty"`
	Alias         string                `json:"alias,omitempty"`

	JobRequests   []JobReq              `json:"jobRequests,omitempty"`
	ExpectedProc  int                   `json:"expectedProc,omitempty"`
	procs         map[string]*Process
	jobs          map[string]*Job

}

type JobReq struct {
	Name               string                `json:"name"`
	TotalTasks         int                   `json:"totalTasks"`
	MaxTaskAllocation  int                   `json:"maxTaskAllocation,omitempty"`
	MinProcesses       int                   `json:"minProcesses,omitempty"`
	MaxProcesses       int                   `json:"maxProcesses,omitempty"`
	Metadata           interface{}           `json:"metadata,omitempty"`
	IsMultiVersion     bool                  `json:"isMultiVersion,omitempty"`
}

func (d *Deployment) GetProcs() map[string]*Process {
	return d.procs
}

func (d *Deployment) GetJobs() map[string]*Job {
	return d.jobs
}

func (d *Deployment) SumAllocation() (alloc, required int) {
	alloc = 0
	required = 0
	for _, job := range d.jobs {
		alloc += job.ExpectedProc
		required += job.MinProcesses
	}
	return
}

type DeploymentMap struct {
	Deployments   map[string][]*Deployment
	logger        nuclio.Logger
	ctx           *ManagerContext
}

func NewDeploymentMap(logger nuclio.Logger, context *ManagerContext) (*DeploymentMap, error) {
	newDeploymentMap := DeploymentMap{logger:logger, ctx:context}
	newDeploymentMap.Deployments = map[string][]*Deployment{}
	return &newDeploymentMap, nil
}

func NewDeployment(newDeployment *Deployment) *Deployment {
	newDeployment.procs = map[string]*Process{}
	newDeployment.jobs  = map[string]*Job{}
	return newDeployment
}


func (dm *DeploymentMap) UpdateDeployment(deployment *Deployment) error {

	if deployment.Namespace == "" {
		deployment.Namespace = "default"
	}
	dm.logger.DebugWith("Update Deployment", "deployment", deployment)

	// find the deployment list for the desired namespace/function (w/o version)
	list, ok := dm.Deployments[deployment.Namespace + "." + deployment.Function]
	if !ok {
		// if not found create a new deployment list
		newList := []*Deployment{NewDeployment(deployment)}
		dm.Deployments[deployment.Namespace + "." + deployment.Function] = newList
		return nil
	}

	// look for a specific deployment matching the version number, if found update it
	for _, dep := range list {
		if dep.Version == deployment.Version {
			// check if for some reason the name in the deployment object changed
			if dep.Name != "" && dep.Name != deployment.Name {
				dm.logger.WarnWith("Deployment name changed",
					"namespace", dep.Namespace, "function", dep.Function, "version", dep.Version,
					"old-name", dep.Name, "new-name", deployment.Name)
			}
			dep.Name = deployment.Name

			// check if the deployment Alias changed (may need to re-route events)
			if dep.Alias != deployment.Alias {
				// TODO: handle alias change, POD may already restart w new Alias
				dm.logger.WarnWith("Deployment alias changed",
					"namespace", dep.Namespace, "function", dep.Function, "version", dep.Version,
					"old-alias", dep.Alias, "new-alias", deployment.Alias)

				dep.Alias = deployment.Alias
			}

			// check if the deployment scale changed
			if dep.ExpectedProc != deployment.ExpectedProc {
				// TODO: handle ExpectedProc change (rebalance)
				dm.logger.DebugWith("Deployment scale changed",
					"namespace", dep.Namespace, "function", dep.Function, "version", dep.Version,
					"old-scale", dep.ExpectedProc, "new-scale", deployment.ExpectedProc)

				dep.ExpectedProc = deployment.ExpectedProc
			}

			return nil
		}
	}

	// if its a new deployment add it to the list
	// TODO: validate values, config/create jobs
	list = append(list, deployment)
	return nil
}

// return a filtered list of deployments (for portal)
func (dm *DeploymentMap) GetAllDeployments(namespace, function string) []*Deployment {
	list := []*Deployment{}
	for key, deps := range dm.Deployments {
		split := strings.Split(key, ".")
		// TODO: filter by function , if both ns & function direct lookup deps
		if namespace == "" || namespace == split[0] {
			list = append(list, deps...)
		}
	}

	return list
}

// return a specific deployment by namespace, function name, and version (or alias)
func (dm *DeploymentMap) FindDeployment(namespace, function, version string, withAliases bool) *Deployment {
	list, ok := dm.Deployments[namespace + "." + function]
	if !ok {
		return nil
	}

	if version == "" {
		version = "latest"
	}

	for _, dep := range list {
		if (dep.Version == version) || (withAliases && dep.Alias == version) {
			return dep
		}
	}

	return nil
}

// List jobs assigned to namespace/function, all if no version specified or by version
func (dm *DeploymentMap) ListJobs(namespace, function, version string) []*Job {
	list := []*Job{}

	deps := dm.GetAllDeployments(namespace, function)
	for _, dep := range deps {
		for _, job := range dep.GetJobs() {
			if version == "" || version == job.Version {
				list = append(list, job)
			}
		}
	}

	return list
}

// ?? TODO: broken, unused
func (dm *DeploymentMap) RemoveDeployment(namespace, function, version string) error {
	list, ok := dm.Deployments[namespace + "." + function]
	if !ok {
		return nil
	}

	for i, dep := range list {
		if dep.Version == version  {
			dm.Deployments[namespace + "." + function] = append(list[:i], list[i+1:]...)
			return nil
		}
	}

	return nil
}

// Handle process update notifications (e.g. new/update PODs)
func (dm *DeploymentMap) UpdateProcess(proc *Process) error {

	dm.logger.DebugWith("Update Process", "process", proc)

	dep := dm.FindDeployment(proc.Namespace, proc.Function, proc.Version, false)

	if dep == nil {
		dm.logger.WarnWith("Deployment wasnt found in process update",
			"namespace", proc.Namespace, "function", proc.Function, "version", proc.Version)

		dep = NewDeployment(&Deployment{
			Namespace: proc.Namespace, Function: proc.Function, Version: proc.Version})
		dep.procs[proc.Name] = proc
		dm.UpdateDeployment(dep)

		return nil
	}

	// TODO: if new - validate, assign to jobs based on expected (per job) vs actual
	// is it ready or just started in k8s
	dep.procs[proc.Name] = proc
	return nil
}

// handle process removal, unused (TODO: POD delete or based on periodic scan/heatlh if no update reported in T time )
func (dm *DeploymentMap) RemoveProcess(proc *Process) error {
	dep := dm.FindDeployment(proc.Namespace, proc.Function, proc.Version, false)

	if dep == nil {
		dm.logger.WarnWith("Deployment wasnt found in process remove",
			"namespace", proc.Namespace, "function", proc.Function, "version", proc.Version)
		return nil
	}

	delete(dep.procs, proc.Name)
	return nil
}

// create new job, TODO: remove allow jobs only from new deploy
func (dm *DeploymentMap) JobRequest(job *Job) error {

	dm.logger.DebugWith("Job request", "job", job)

	dep := dm.FindDeployment(job.Namespace, job.Function, job.Version, true)

	if dep == nil {
		dm.logger.WarnWith("Deployment wasnt found in job request",
			"namespace", job.Namespace, "function", job.Function, "version", job.Version)

		ver := job.Version
		if ver == "" {
			ver = "latest"
		}
		_, err := strconv.Atoi(ver)
		if ver != "latest" && err != nil {
			// if ver != latest and its not an Int its an alias, must have a deploy to continue
			return fmt.Errorf("Function with alias %s was not found", ver)
		}

		// create a new deployment stub from the job information and return (no resources for it yet)
		dep = NewDeployment(&Deployment{Namespace: job.Namespace, Function: job.Function, Version: ver})
		dep.jobs[job.Name] = job
		dm.UpdateDeployment(dep)

		return nil
	}

	_, ok := dep.jobs[job.Name]
	if ok {
		return fmt.Errorf("Job named %s already exist", job.Name)
	}

	return dm.addJob(job, dep)
}

// TODO: fix to only work w new deploy , may not be needed
func (dm *DeploymentMap) addJob(job *Job, dep *Deployment) error {

	alloc, requiered := dep.SumAllocation()
	dm.logger.DebugWith("SumAllocation", "alloc", alloc, "req", requiered)

	if job.MinProcesses <= dep.ExpectedProc - alloc {
		dep.jobs[job.Name] = job
		job.ExpectedProc = dep.ExpectedProc - alloc
		if job.MaxProcesses > 0 && job.ExpectedProc > job.MaxProcesses {
			job.ExpectedProc = job.MaxProcesses
		}

		fmt.Println("out7",dep.jobs)
		return nil
	}

	//TODO: allocation w rebalance logic

	return fmt.Errorf("No resources for job %s", job.Name)
}

// TODO: unused
func (dm *DeploymentMap) RemoveJob(job *Job) error {

	dep := dm.FindDeployment(job.Namespace, job.Function, job.Version, true)

	if dep == nil {
		dm.logger.WarnWith("Deployment wasnt found in job remove",
			"namespace", job.Namespace, "function", job.Function, "version", job.Version)
		return nil
	}


	//TODO: handle pending & rebalancing

	delete(dep.jobs, job.Name)
	return nil
}

// read/update jobs from deployment TODO: assign expected procs per each , no need for update for now (stash it)
func (dm *DeploymentMap) updateDeployJobs(dep *Deployment) error {
	for _, rjob := range dep.JobRequests {

		_, ok := dep.jobs[rjob.Name]
		if !ok {
			newJob := &Job{Name:dep.Name, Namespace:dep.Namespace,
				Function:dep.Function, Version:dep.Version,
				TotalTasks:rjob.TotalTasks, MaxTaskAllocation:rjob.MaxTaskAllocation,
				MaxProcesses:rjob.MaxProcesses, MinProcesses:rjob.MinProcesses,
			}
			job, err := NewJob(&dm.ctx, newJob)
			if err != nil {
				dm.logger.ErrorWith("Failed to create a job", "deploy", dep.Name, "job", rjob.Name, "err", err)
			}
			err = dm.addJob(job, dep)
			if err != nil {
				dm.logger.ErrorWith("Failed on adding job to deployment", "deploy", dep.Name, "job", rjob.Name, "err", err)
			}

		}

		// TODO: handle update and delete existing job
	}

	return nil
}