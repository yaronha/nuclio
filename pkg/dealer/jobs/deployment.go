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

	ExpectedProc  int                   `json:"expectedProc,omitempty"`
	RequieredProc int                   `json:"requieredProc,omitempty"`
	procs         map[string]*Process
	jobs          map[string]*Job

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
	deployments map[string][]*Deployment
	logger        nuclio.Logger
	ctx           *ManagerContext
}

func NewDeploymentMap(logger nuclio.Logger, context *ManagerContext) (*DeploymentMap, error) {
	newDeploymentMap := DeploymentMap{logger:logger, ctx:context}
	newDeploymentMap.deployments = map[string][]*Deployment{}
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

	list, ok := dm.deployments[deployment.Namespace + "." + deployment.Function]
	if !ok {
		newList := []*Deployment{NewDeployment(deployment)}
		dm.deployments[deployment.Namespace + "." + deployment.Function] = newList
		return nil
	}

	for _, dep := range list {
		if dep.Version == deployment.Version {
			if dep.Name != "" && dep.Name != deployment.Name {
				dm.logger.WarnWith("Deployment name changed",
					"namespace", dep.Namespace, "function", dep.Function, "version", dep.Version,
					"old-name", dep.Name, "new-name", deployment.Name)
			}
			dep.Name = deployment.Name

			if dep.Alias != deployment.Alias {
				// TODO: handle alias change, POD may already restart w new Alias
				dm.logger.WarnWith("Deployment alias changed",
					"namespace", dep.Namespace, "function", dep.Function, "version", dep.Version,
					"old-alias", dep.Alias, "new-alias", deployment.Alias)

				dep.Alias = deployment.Alias
			}

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

	list = append(list, deployment)
	return nil
}

func (dm *DeploymentMap) GetAllDeployments(namespace, function string) []*Deployment {
	list := []*Deployment{}
	for key, deps := range dm.deployments {
		split := strings.Split(key, ".")
		if namespace == "" || namespace == split[0] {
			list = append(list, deps...)
		}
	}

	return list
}

func (dm *DeploymentMap) FindDeployment(namespace, function, version string, withAliases bool) *Deployment {
	list, ok := dm.deployments[namespace + "." + function]
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

func (dm *DeploymentMap) RemoveDeployment(namespace, function, version string) error {
	list, ok := dm.deployments[namespace + "." + function]
	if !ok {
		return nil
	}

	for i, dep := range list {
		if dep.Version == version  {
			dm.deployments[namespace + "." + function] = append(list[:i], list[i+1:]...)
			return nil
		}
	}

	return nil
}

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

	dep.procs[proc.Name] = proc
	return nil
}

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
			return fmt.Errorf("Function with alias %s was not found", ver)
		}

		dep = NewDeployment(&Deployment{Namespace: job.Namespace, Function: job.Function, Version: ver})
		dep.jobs[job.Name] = job
		dm.UpdateDeployment(dep)

		return nil
	}

	_, ok := dep.jobs[job.Name]
	if ok {
		return fmt.Errorf("Job named %s already exist", job.Name)
	}
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
