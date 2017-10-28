package jobs

import (
	"github.com/nuclio/nuclio-sdk"
	"strings"
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
		err := dep.updateJobs()
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

					// TODO: change deployment expected (and rebalance)

					dep.ExpectedProc = deployment.ExpectedProc

				}
			}

			return nil
		}
	}

	// if its a new deployment add it to the list
	dep := dm.NewDeployment(deployment)
	err := dep.updateJobs()
	if err != nil {
		dm.logger.ErrorWith("Failed to update jobs in deployment", "deploy", dep.Name, "err", err)
		return err
	}
	dm.Deployments[key] = append(dm.Deployments[key], dep)
	return nil
}


// return a filtered list of deployments (for portal)
func (dm *DeploymentMap) GetAllDeployments(namespace, function, version string) []*Deployment {
	list := []*Deployment{}
	for key, deps := range dm.Deployments {
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



