package jobs

import (
	"github.com/nuclio/nuclio-sdk"
	"strings"
)

type DeploymentMap struct {
	deployments map[string][]*Deployment
	logger      nuclio.Logger
	ctx         *ManagerContext
}

func NewDeploymentMap(logger nuclio.Logger, context *ManagerContext) (*DeploymentMap, error) {
	newDeploymentMap := DeploymentMap{
		logger: logger.GetChild("depMap").(nuclio.Logger),
		ctx:    context}
	newDeploymentMap.deployments = map[string][]*Deployment{}
	return &newDeploymentMap, nil
}

func (dm *DeploymentMap) NewDeployment(deploymentSpec *DeploymentSpec) *Deployment {
	newDeployment := &Deployment{BaseDeployment: deploymentSpec.BaseDeployment}
	newDeployment.dm = dm
	newDeployment.procs = map[string]*Process{}
	newDeployment.jobs = map[string]*Job{}
	return newDeployment
}

func (dm *DeploymentMap) UpdateDeployment(newDep *DeploymentSpec) (*Deployment, error) {

	if newDep.Namespace == "" {
		newDep.Namespace = "default"
	}
	dm.logger.DebugWith("Update Deployment", "deployment", newDep)

	// find the deployment list for the desired namespace/function (w/o version)
	key := newDep.Namespace + "." + newDep.Function
	list, ok := dm.deployments[key]
	if !ok {
		// if not found create a new deployment list
		dep := dm.NewDeployment(newDep)
		err := dep.updateJobs(newDep.Triggers)
		if err != nil {
			dm.logger.ErrorWith("Failed to update jobs in deployment", "deploy", dep.Name, "err", err)
			return nil, err
		}

		newList := []*Deployment{dep}
		dm.deployments[key] = newList
		return dep, nil
	}

	// look for a specific deployment matching the version number, if found update it
	for _, dep := range list {
		if dep.Version == newDep.Version {
			// check if for some reason the name in the deployment object changed
			if dep.Name != "" && dep.Name != newDep.Name {
				dm.logger.WarnWith("Deployment name changed",
					"namespace", dep.Namespace, "function", dep.Function, "version", dep.Version,
					"old-name", dep.Name, "new-name", newDep.Name)
			}
			dep.Name = newDep.Name

			// check if the deployment Alias changed (may need to re-route events)
			if dep.Alias != newDep.Alias {
				// TODO: handle alias change, POD may already restart w new Alias
				dm.logger.WarnWith("Deployment alias changed",
					"namespace", dep.Namespace, "function", dep.Function, "version", dep.Version,
					"old-alias", dep.Alias, "new-alias", newDep.Alias)

				dep.Alias = newDep.Alias
			}

			// check if the deployment scale changed
			if dep.ExpectedProc != newDep.ExpectedProc {
				dm.logger.DebugWith("Deployment scale changed",
					"namespace", dep.Namespace, "function", dep.Function, "version", dep.Version,
					"old-scale", dep.ExpectedProc, "new-scale", newDep.ExpectedProc)

				oldValue := dep.ExpectedProc
				dep.ExpectedProc = newDep.ExpectedProc
				if dep.ExpectedProc != 0 && dep.ExpectedProc > oldValue && len(dep.procs) > 0 {
					// dont re-balance if we scale-down, wait for processes to stop
					dep.Rebalance()
				}
			}

			// TODO: if function gen changed updateJobs, handle removed
			if newDep.FuncGen != "" && dep.FuncGen != newDep.FuncGen {
				dep.FuncGen = newDep.FuncGen

				// Identify if jobs were removed and remove them (if were created by the deployment)
				jobMap := map[string]bool{}
				for _, job := range newDep.Triggers {
					jobMap[job.Name] = true
				}
				for jobName, job := range dep.jobs {
					if _, found := jobMap[jobName]; !found && job.FromDeployment() {
						dep.RemoveJob(job, false)
					}
				}

				err := dep.updateJobs(newDep.Triggers)
				if err != nil {
					dm.logger.ErrorWith("Failed to update jobs in deployment", "deploy", newDep.Name, "err", err)
					return nil, err
				}

			}

			return dep, nil
		}
	}

	// if its a new deployment add it to the list
	dep := dm.NewDeployment(newDep)
	err := dep.updateJobs(newDep.Triggers)
	if err != nil {
		dm.logger.ErrorWith("Failed to update jobs in deployment", "deploy", newDep.Name, "err", err)
		return nil, err
	}
	dm.deployments[key] = append(dm.deployments[key], dep)
	return dep, nil
}

// return a filtered list of deployments (for portal)
func (dm *DeploymentMap) GetAllDeployments(namespace, function, version string) []*Deployment {
	list := []*Deployment{}
	for key, deps := range dm.deployments {
		split := strings.Split(key, ".")
		// TODO: filter by function , if both ns & function direct lookup deps
		if namespace == "" || namespace == split[0] {
			for _, dep := range deps {
				if version == "" || version == dep.Version || version == dep.Alias {
					list = append(list, dep)
				}
			}
		}
	}

	return list
}

// return a specific deployment by namespace, function name, and version (or alias)
func (dm *DeploymentMap) FindDeployment(namespace, function, version string, withAliases bool) *Deployment {
	key := namespace + "." + function
	list, ok := dm.deployments[key]
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

//  TODO: need to complete the remove deploy
func (dm *DeploymentMap) RemoveDeployment(namespace, function, version string) error {
	key := namespace + "." + function
	list, ok := dm.deployments[key]
	if !ok {
		return nil
	}

	for i, dep := range list {
		if dep.Version == version {
			dm.logger.DebugWith("Removing Deployment", "deployment", dep)
			err := dep.ClearDeployment()
			if err != nil {
				dm.logger.ErrorWith("Failed to clear deployment", "deploy", dep.Name, "err", err)
				return err
			}
			newList := append(list[:i], list[i+1:]...)
			if len(newList) == 0 {
				delete(dm.deployments, key)
			} else {
				dm.deployments[key] = newList
			}
			return nil
		}
	}

	return nil
}
