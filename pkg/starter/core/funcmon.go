package core

import (
	"fmt"
	"github.com/valyala/fasthttp"
	"strings"
	"github.com/nuclio/logger"
)

func NewFuncDirectory(logger logger.Logger) *FuncDirectory {
	newDir := FuncDirectory{logger: logger.GetChild("funcdict")}
	newDir.functions = map[string]*FunctionRecord{}
	newDir.Radix = NewPathRadix(logger)
	return &newDir
}

type FuncDirectory struct {
	functions  map[string]*FunctionRecord
	Radix      *PathRadix
	logger     logger.Logger
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
		request.ReturnChan <- &LookupResponse{HostClient: fn.hostClient,  DestURL: fn.getFunctionURL(), DestFunction: fn.Function + ":" + fn.Version}
		return nil
	}

	fn.requests = append(fn.requests, request)
	// TODO: re-enable the function if needed


	return nil
}

func (fd *FuncDirectory) UpdateFunction(fn *FunctionBase) {
	//TODO: detect state change
	function, ok := fd.functions[getFuncKey(fn.Namespace, fn.Function, fn.Version)]

	if !ok {
		function = &FunctionRecord{FunctionBase: *fn}
		fd.functions[getFuncKey(fn.Namespace, fn.Function, fn.Version)] = function
		fd.Radix.UpdatePaths(function)
		return
	}

	if fn.Gen != function.Gen {
		// TODO: detect deleted paths and remove from radix
		function.Disabled = fn.Disabled
		function.Ingresses = fn.Ingresses
		function.CanScaledown = fn.CanScaledown
		function.Gen = fn.Gen
		fd.functions[getFuncKey(fn.Namespace, fn.Function, fn.Version)] = function
		fd.Radix.UpdatePaths(function)
	}

}

func (fd *FuncDirectory) DeleteFunction(fn *FunctionBase) error {
	//TODO: detect state change
	function, ok := fd.functions[getFuncKey(fn.Namespace, fn.Function, fn.Version)]

	if !ok {
		fd.logger.ErrorWith("function not found in DeleteFunction", "func", fn.Function, "ver", fn.Version)
		return fmt.Errorf("function not found in DeleteFunction")
	}

	err := fd.Radix.DeletePaths(function)
	if err != nil {
		return err
	}

	delete(fd.functions, getFuncKey(fn.Namespace, fn.Function, fn.Version))

	return nil
}

func (fd *FuncDirectory) UpdateEndPoints(eps *FunctionEndPoints) error {

	function, ok := fd.functions[getFuncKey(eps.Namespace, eps.Name, eps.Version)]
	if !ok {
		return fmt.Errorf("Function %s/%s:%s not found", eps.Namespace, eps.Name, eps.Version)
	}

	restarted := (len(function.EndPoints) == 0) && (len(eps.IPs) > 0)

	function.EndPoints = eps.IPs
	function.ApiPort = eps.APIPort
	function.controlPort = eps.ControlPort

	addrList := []string{}
	for _, ip := range eps.IPs {
		addrList = append(addrList, fmt.Sprintf("%s:%d", ip, eps.APIPort))
	}
	function.hostClient = &fasthttp.HostClient{ Addr: strings.Join(addrList, ",")}

	if restarted {
		function.releaseRequests()
	}

	return nil

}
