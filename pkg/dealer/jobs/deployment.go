package jobs

import (
	"github.com/nuclio/nuclio-sdk"
	"fmt"
	"strconv"
)

type Deployment struct {
	Namespace     string                `json:"namespace"`
	Function      string                `json:"function"`
	Name          string                `json:"name"`
	Version       string                `json:"version,omitempty"`
	Alias         string                `json:"alias,omitempty"`

	ExpectedProc  int
	RequieredProc int
	procs         map[string]*Process
	jobs          map[string]*Job

}


type DeploymentMap struct {
	deployments map[string][]*Deployment
	logger        nuclio.Logger
	ctx           *ManagerContext
}

func NewDeploymentMap(logger nuclio.Logger, context *ManagerContext) (*DeploymentMap, error) {
	newDeploymentMap := DeploymentMap{logger:logger, ctx:context}
	return &newDeploymentMap, nil
}

func NewDeployment(namespace, function, version string) *Deployment {
	newDeployment := Deployment{Namespace:namespace, Function:function, Version:version}
	newDeployment.procs = map[string]*Process{}
	newDeployment.jobs  = map[string]*Job{}
	return &newDeployment
}


func (dm *DeploymentMap) UpdateDeployment(deployment *Deployment) error {
	list, ok := dm.deployments[deployment.Namespace + "." + deployment.Function]
	if !ok {
		newList := []*Deployment{deployment}
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
	dep := dm.FindDeployment(proc.Namespace, proc.Function, proc.Version, false)

	if dep == nil {
		dm.logger.WarnWith("Deployment wasnt found in process update",
			"namespace", proc.Namespace, "function", proc.Function, "version", proc.Version)

		dep = NewDeployment(proc.Namespace, proc.Function, proc.Version)
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

		dep = NewDeployment(job.Namespace, job.Function, ver)
		dep.jobs[job.Name] = job
		dm.UpdateDeployment(dep)

		return nil
	}

	//TODO: allocation logic

	return nil
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
