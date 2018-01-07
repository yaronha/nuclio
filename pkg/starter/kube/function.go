package kube

import (
	"github.com/nuclio/nuclio/pkg/platform/kube/functioncr"
	"github.com/pkg/errors"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

func (fi *functionIfc) Disable(name string, state bool) error {
	function, err := fi.kc.functionCR.Get(fi.kc.namespace, name)
	fi.kc.logger.DebugWith("Disable function", "fn", function)

	function.Spec.Disabled = state

	_, err = fi.kc.functionCR.Update(function)
	if err != nil {
		return errors.Wrap(err, "Failed to disable function")
	}

	return nil
}

func (fi *functionIfc) LoadFunctions() error {

	opts := meta_v1.ListOptions{
		//LabelSelector: NUCLIO_SELECTOR,
	}

	functions, err := fi.kc.functionCR.List(fi.kc.namespace, &opts)
	if err != nil {
		return errors.Wrap(err, "Failed to list functions")
	}

	for _, fn := range functions.Items {
		canScaledown := fi.onlyHttpTriggers(&fn) && (fn.Spec.MinReplicas == 0)
		function := core.FunctionBase{
			Namespace: fn.Namespace, Name:fn.Name, Version: fn.Labels["version"], Gen: fn.ResourceVersion,
			Disabled: fn.Spec.Disabled, Ingresses: fi.parseTriggers(&fn), CanScaledown: canScaledown,
		}
		fi.kc.reqChannel <- &core.AsyncRequests{ Type: core.RequestTypeUpdateFunction, Data: &function}
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