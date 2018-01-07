package kube

import (
	"github.com/nuclio/nuclio/pkg/platform/kube/functioncr"
	"github.com/pkg/errors"
	"github.com/nuclio/nuclio/pkg/functionconfig"
	"github.com/nuclio/nuclio/pkg/starter/core"
)

func NewFunctionInterface(kc *kubeClient) (*functionIfc, error) {
	return &functionIfc{kc:kc}, nil
}

type functionIfc struct {
	kc     *kubeClient
}

func (fi *functionIfc) Get(name string) (*functioncr.Function, error) {
	function, err := fi.kc.functionCR.Get(fi.kc.namespace, name)
	fi.kc.logger.DebugWith("Get function", "fn", function)

	return function, err
}

func (fi *functionIfc) Watch() (chan functioncr.Change, error) {

	changeChan := make(chan functioncr.Change, 100)
	_, err := fi.kc.functionCR.WatchForChanges(fi.kc.namespace, changeChan)
	fi.kc.logger.Debug("Started function watch")

	return changeChan, err
}

func (fi *functionIfc) FuncCRtoFunction(fn *functioncr.Function) *core.FunctionBase {

	canScaledown := fi.onlyHttpTriggers(fn) && (fn.Spec.MinReplicas == 0)
	function := core.FunctionBase{
		Namespace: fn.Namespace, Function: fn.Labels["name"], CRName: fn.Name,
		Version: fn.Labels["version"], Gen: fn.ResourceVersion,
		Disabled: fn.Spec.Disabled, Ingresses: fi.parseTriggers(fn), CanScaledown: canScaledown,
	}

	return &function
}

func (fi *functionIfc) Disable(crName string, state bool) error {
	function, err := fi.kc.functionCR.Get(fi.kc.namespace, crName)
	fi.kc.logger.DebugWith("Disable function", "fn", function)

	function.Spec.Disabled = state

	_, err = fi.kc.functionCR.Update(function)
	if err != nil {
		return errors.Wrap(err, "Failed to disable function")
	}

	return nil
}


func (fi *functionIfc) parseTriggers(function *functioncr.Function) []functionconfig.Ingress {
	allPaths := []functionconfig.Ingress{}

	for _, ingress := range functionconfig.GetIngressesFromTriggers(function.Spec.Triggers) {
		allPaths = append(allPaths, ingress)
	}

	if len(allPaths) == 0 {
		defaultPath := "/"+ function.Name + "/" + function.Labels["version"]
		allPaths = append(allPaths, functionconfig.Ingress{Host:"", Paths:[]string{defaultPath}})
	}

	return allPaths
}

func (fi *functionIfc) onlyHttpTriggers(function *functioncr.Function) bool {
	for _, trigger := range function.Spec.Triggers {
		if trigger.Kind != "http" {
			return false
		}
	}

	return true
}