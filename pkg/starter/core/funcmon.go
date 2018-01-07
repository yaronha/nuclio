package core

import (
	"fmt"
)

func NewFuncDirectory() *FuncDirectory {
	newDir := FuncDirectory{}
	newDir.functions = map[string]*FunctionRecord{}
	newDir.Radix = NewPathRadix()
	return &newDir
}

type FuncDirectory struct {
	functions  map[string]*FunctionRecord
	Radix      *PathRadix
}

func (fd *FuncDirectory) GetFunctions() map[string]*FunctionRecord {
	return fd.functions
}

func (fd *FuncDirectory) FunctionLookup(request *LookupRequest) error {

	fn, found:=fd.Radix.Lookup(request.Host, request.Path)
	if !found {
		request.ReturnChan <- &LookupResponse{NotFound:true}
		return fmt.Errorf("Path %s/%s not found", request.Host, request.Path)
	}

	if fn.IsReady() {
		request.ReturnChan <- &LookupResponse{DestURL: fn.getFunctionURL(), DestFunction: fn.Name + ":" + fn.Version}
		return nil
	}

	fn.deferRequest(request)

	return nil
}

func (fd *FuncDirectory) UpdateFunction(fn *FunctionBase) {
	//TODO: detect state change
	function, ok := fd.functions[getFuncKey(fn.Namespace, fn.Name, fn.Version)]

	if !ok {
		function = &FunctionRecord{FunctionBase: *fn}
		fd.functions[getFuncKey(fn.Namespace, fn.Name, fn.Version)] = function
		fd.Radix.UpdatePaths(function)
		return
	}

	if fn.Gen != function.Gen {
		// TODO: detect deleted paths and remove from radix
		function.Disabled = fn.Disabled
		function.Ingresses = fn.Ingresses
		function.CanScaledown = fn.CanScaledown
		function.Gen = fn.Gen
		fd.functions[getFuncKey(fn.Namespace, fn.Name, fn.Version)] = function
		fd.Radix.UpdatePaths(function)
	}

}

func (fd *FuncDirectory) UpdateEndPoints(eps *FunctionEndPoints) error {

	function, ok := fd.functions[getFuncKey(eps.Namespace, eps.Name, eps.Version)]
	if !ok {
		return fmt.Errorf("Function %s/%s:%s not found", eps.Namespace, eps.Name, eps.Version)
	}

	restarted := (len(function.endPoints) == 0) && (len(eps.IPs) > 0)

	function.endPoints = eps.IPs
	function.apiPort = eps.APIPort
	function.controlPort = eps.ControlPort

	if restarted {
		function.releaseRequests()
	}

	return nil

}
