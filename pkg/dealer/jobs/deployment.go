package jobs

import (
	"github.com/nuclio/nuclio-sdk"
	"fmt"
	"strconv"
	"strings"
	"github.com/pkg/errors"
)

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

func (dm *DeploymentMap) NewDeployment(newDeployment *Deployment) *Deployment {
	newDeployment.dm = dm
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
	key := deployment.Namespace + "." + deployment.Function
	list, ok := dm.Deployments[key]
	if !ok {
		// if not found create a new deployment list
		dep := dm.NewDeployment(deployment)
		err := dm.updateDeployJobs(dep)
		if err != nil {
			dm.logger.ErrorWith("Failed to update jobs in deployment", "deploy", dep.Name, "err", err)
			return err
		}

		newList := []*Deployment{dep}
		dm.Deployments[key] = newList
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

				if dep.ExpectedProc != deployment.ExpectedProc {

					// TODO: chanhe jobs expected and rebalance

					dep.ExpectedProc = deployment.ExpectedProc

				}
			}

			return nil
		}
	}

	// if its a new deployment add it to the list
	dep := dm.NewDeployment(deployment)
	err := dm.updateDeployJobs(dep)
	if err != nil {
		dm.logger.ErrorWith("Failed to update jobs in deployment", "deploy", dep.Name, "err", err)
		return err
	}
	dm.Deployments[key] = append(dm.Deployments[key], dep)
	return nil
}

// read/update jobs from deployment TODO: assign expected procs per each , no need for update for now (stash it)
func (dm *DeploymentMap) updateDeployJobs(dep *Deployment) error {
	for _, rjob := range dep.JobRequests {

		_, ok := dep.jobs[rjob.Name]
		if !ok {
			newJob := &Job{Name:rjob.Name, Namespace:dep.Namespace,
				Function:dep.Function, Version:dep.Version,
				TotalTasks:rjob.TotalTasks, MaxTaskAllocation:rjob.MaxTaskAllocation,
			}
			job, err := NewJob(dm.ctx, newJob)
			if err != nil {
				dm.logger.ErrorWith("Failed to create a job", "deploy", dep.Name, "job", rjob.Name, "err", err)
			}

			// TODO: change for multi-job per dep & proc
			dep.jobs[rjob.Name] = job


		}

	}

	for _, proc := range dep.procs {
		err := dep.AllocateTasks(proc)
		if err != nil {
			dm.logger.ErrorWith("Failed to allocate jobtasks to proc", "deploy", dep.Name, "proc", proc.Name, "err", err)
		}
	}

	dm.logger.DebugWith("updateDeployJobs", "jobs", dep.jobs)
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
		dm.logger.DebugWith("FindDeployment - array not found", "namespace", namespace, "func", function)
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

	dm.logger.DebugWith("FindDeployment - ver not found", "namespace", namespace, "func", function, "ver", version)
	return nil
}

// List jobs assigned to namespace/function, all if no version specified or by version
func (dm *DeploymentMap) ListJobs(namespace, function, version string) []*JobMessage {
	list := []*JobMessage{}

	deps := dm.GetAllDeployments(namespace, function)
	for _, dep := range deps {
		for _, job := range dep.GetJobs() {
			if version == "" || version == job.Version {
				list = append(list, job.GetJobState())
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

		dep = dm.NewDeployment(&Deployment{
			Namespace: proc.Namespace, Function: proc.Function, Version: proc.Version})
		// TODO: use an AddProcess interface
		dep.AddProcess(proc)
		dm.UpdateDeployment(dep)

		return nil
	}

	// TODO: if new - validate, assign to jobs based on expected (per job) vs actual
	// is it ready or just started in k8s

	dep.AddProcess(proc)
	// TODO: check if proc is in ready state
	// if proc is ready and not assigned to a job assign it to a job with missing procs
	if dep.ExpectedProc > len(dep.procs) {
		err := dep.AllocateTasks(proc)
		if err != nil {
			dm.logger.ErrorWith("Failed to allocate jobtasks to proc", "deploy", dep.Name, "proc", proc.Name, "err", err)
		}
		return nil
	}

	return nil
}




// ======


// handle process removal, unused (TODO: POD delete or based on periodic scan/heatlh if no update reported in T time )
func (dm *DeploymentMap) RemoveProcess(proc *Process) error {
	dep := dm.FindDeployment(proc.Namespace, proc.Function, proc.Version, false)

	if dep == nil {
		dm.logger.WarnWith("Deployment wasnt found in process remove",
			"namespace", proc.Namespace, "function", proc.Function, "version", proc.Version)
		return nil
	}

	dep.procs[proc.Name].Remove()
	delete(dep.procs, proc.Name)

	// TODO: rebalance

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
		dep = dm.NewDeployment(&Deployment{Namespace: job.Namespace, Function: job.Function, Version: ver})
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

	//TODO: allocation w rebalance logic

	return nil
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





type Deployment struct {
	dm            *DeploymentMap
	Namespace     string                `json:"namespace"`
	Function      string                `json:"function"`
	Name          string                `json:"name"`
	Version       string                `json:"version,omitempty"`
	Alias         string                `json:"alias,omitempty"`

	JobRequests   []*JobReq              `json:"jobRequests,omitempty"`
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

func (d *Deployment) AddProcess(proc *Process) {
	d.procs[proc.Name] = proc
	proc.deployment = d
}

func (d *Deployment) GetProcs() map[string]*Process {
	return d.procs
}

func (d *Deployment) GetJobs() map[string]*Job {
	return d.jobs
}

func (d *Deployment) SumTasks() int {
	tasks := 0
	for _, job := range d.jobs {
		tasks += job.TotalTasks
	}
	return tasks
}

// allocate available tasks to process
func (d *Deployment) AllocateTasks(proc *Process) error {

	totalTasks := d.SumTasks()
	tasksPerProc := totalTasks / d.ExpectedProc
	taskReminder := totalTasks - d.ExpectedProc * tasksPerProc
	var totalAsigned, procTasks, aboveMin int


	for _, p := range d.procs {
		procTasks = len(p.GetTasks(true))
		totalAsigned += procTasks
		if procTasks > tasksPerProc {
			aboveMin +=1
		}
	}

	totalUnallocated := totalTasks - totalAsigned
	if totalUnallocated < 0 {
		return fmt.Errorf("Assigned Tasks (%d) greater than Total tasks (%d)",totalAsigned,totalTasks)
	}
	if totalUnallocated == 0 {
		return nil
	}

	alloc := tasksPerProc
	if aboveMin < taskReminder {
		alloc += 1
	}

	//if j.MaxTaskAllocation > 0 && alloc > j.MaxTaskAllocation {
	//	alloc = j.MaxTaskAllocation
	//}

	newAlloc := alloc - len(proc.GetTasks(true))
	if totalUnallocated <= newAlloc {
		newAlloc = totalUnallocated
	}

	_, err := d.addTasks2Proc(proc, newAlloc, tasksPerProc)
	if err !=nil {
		return errors.Wrap(err, "Failed to add tasks")
	}

	return err
}



func (d *Deployment) Rebalance() error {

	totalTasks := d.SumTasks()
	tasksPerProc := totalTasks / d.ExpectedProc
	taskReminder := totalTasks - d.ExpectedProc * tasksPerProc

	var procTasks, missingPlus1, extraPlus1 int
	var tasksUnder, tasksEqual, tasksPlus1, tasksOver []*Process

	for _, p := range d.procs {
		if !p.removingTasks {
			procTasks = len(p.GetTasks(true))
			switch {
			case procTasks < tasksPerProc:
				tasksUnder = append(tasksUnder, p)
			case procTasks == tasksPerProc:
				tasksEqual = append(tasksEqual, p)
			case procTasks == tasksPerProc + 1:
				tasksPlus1 = append(tasksPlus1, p)
			default:
				tasksOver = append(tasksOver, p)
			}
		}
	}

	// desired state is: N with tasksPerProc+1, newnum-N with tasksPerProc (N=taskReminder)
	//   and must not go over MaxAllocation per Job

	missingPlus1 = taskReminder - len(tasksPlus1)
	if missingPlus1 < 0 {
		extraPlus1 = -missingPlus1
		missingPlus1 = 0
	}

	// TODO: StopN balanced across jobs
	for _, p := range tasksOver {
		tasks := p.GetTasks(true)
		if missingPlus1 > 0 {
			p.StopNTasks(len(tasks)-tasksPerProc-1)
			missingPlus1 -= 1
		} else {
			p.StopNTasks(len(tasks)-tasksPerProc)
		}
		_ = p.PushUpdates()
	}

	for _, p := range tasksPlus1 {
		if extraPlus1 > 0 {
			p.StopNTasks(1)
			_ = p.PushUpdates()
			extraPlus1 -= 1
		} else {
			break
		}
	}

	tasksToAdd := append(tasksUnder, tasksEqual...)
	for _, p := range tasksToAdd {
		desired := tasksPerProc
		usedPlus1 := false
		if missingPlus1 > 0 {
			desired += 1
			usedPlus1 = true
		}

		//if j.MaxTaskAllocation != 0 && desired > j.MaxTaskAllocation {
		//	desired = j.MaxTaskAllocation
		//	usedPlus1 = false
		//}

		newAlloc := desired - len(p.GetTasks(true))
		added, err := d.addTasks2Proc(p, newAlloc, tasksPerProc)
		if err !=nil {
			return errors.Wrap(err, "Failed to add tasks")
		}
		if added < newAlloc {
			usedPlus1 = false
		}
		if usedPlus1 {
			missingPlus1 -= 1
		}
		_ = p.PushUpdates()
	}



	return nil
}

func (d *Deployment) addTasks2Proc(proc *Process, toAdd, slice int) (int, error) {
	from := make([]int, len(d.jobs))
	jobIdx := 0
	added :=0
	// TODO improve distribution algo between jobs
	loop:
	for {
		for _, j := range d.jobs {
			toAlloc := j.findUnallocTask(1, &from[jobIdx])
			if len(toAlloc)>0 {
				proc.AddTasks(toAlloc)
				added += 1
				toAdd -= 1
				if toAdd <= 0 {
					break loop
				}

			}
			jobIdx += 1
			if jobIdx == len(d.jobs) {jobIdx=0}
		}
	}

	return added, nil

}