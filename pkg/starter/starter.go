package starter

import (
	"github.com/nuclio/nuclio-sdk"
	"github.com/nuclio/nuclio/pkg/starter/core"
	"github.com/nuclio/nuclio/pkg/starter/kube"
	"github.com/pkg/errors"
	"time"
	"github.com/nuclio/nuclio/pkg/platform/kube/functioncr"
	"github.com/valyala/fasthttp"
	"bytes"
	"fmt"
)

const LISETEN_ON_PORT = "30088"

func NewStarter(logger nuclio.Logger, config StarterConfig) (*Starter, error) {
	newStarter := Starter{logger:logger}
	newStarter.RequestsChannel = make(chan *core.AsyncRequests, 100)
	newStarter.config = &config

	return &newStarter, nil

}

type StarterConfig struct {
	Verbose     bool
	Kubeconf    string
	Namespace   string
}

type Starter struct {
	logger              nuclio.Logger
	config              *StarterConfig
	funcDB              *core.FuncDirectory
	RequestsChannel     chan *core.AsyncRequests
}

func (s *Starter) Start() error {

	kc, err := kube.NewKubeClient(s.logger, s.config.Kubeconf, s.config.Namespace, s.RequestsChannel)
	if err != nil {
		return errors.Wrap(err, "Failed to init Kubernetes client")
	}

	s.funcDB = core.NewFuncDirectory()

	functionChanges, err := kc.Function.Watch()
	if err != nil {
		return errors.Wrap(err, "Failed to watch functions")
	}

	time.Sleep(time.Second)

	err = kc.NewEPWatcher()
	if err != nil {
		return errors.Wrap(err, "Failed to watch endpoints")
	}

	go func() {
		for {

			select {

			case change := <- functionChanges:

				switch change.Kind {
				case functioncr.ChangeKindAdded, functioncr.ChangeKindUpdated :

					function := kc.Function.FuncCRtoFunction(change.Function)
					s.logger.DebugWith("Update Function", "func", function)
					s.funcDB.UpdateFunction(function)

				case functioncr.ChangeKindDeleted:

					s.logger.DebugWith("Function deleted", "function", change.Function.Name)


				}


			case req := <- s.RequestsChannel:

				switch req.Type {

				case core.RequestTypeLookup:

					request := req.Data.(*core.LookupRequest)
					s.logger.DebugWith("Function Lookup", "host", request.Host, "path", request.Path)
					s.funcDB.FunctionLookup(request)

				case core.RequestTypeUpdateEndPoints:

					eps := req.Data.(*core.FunctionEndPoints)
					s.logger.DebugWith("Update EndPoints", "eps", eps)
					s.funcDB.UpdateEndPoints(eps)

				}


			}

		}
	}()


	fmt.Printf("Listening on port : %s\n", LISETEN_ON_PORT)
	if err := fasthttp.ListenAndServe(":" + LISETEN_ON_PORT, s.reverseProxyHandler); err != nil {
		errors.Wrap(err,"error in fasthttp server: %s")
	}


	return nil

}


func (s *Starter) reverseProxyHandler(ctx *fasthttp.RequestCtx) {
	req := &ctx.Request
	resp := &ctx.Response
	prepareRequest(req)

	host := bytes.Split(req.Host(), []byte(":"))[0]

	retChan := make(chan *core.LookupResponse)
	lookupReq := &core.LookupRequest{ Host:string(host), Path:string(req.RequestURI()), ReturnChan: retChan}
	s.RequestsChannel <- &core.AsyncRequests{ Type: core.RequestTypeLookup, Data: lookupReq}

	ep := <- retChan
	if ep.NotFound {
		resp.SetStatusCode(503)
		resp.SetBody([]byte("End point address was not found!"))
		return
	}
	s.logger.DebugWith("Lookup resp", "notfound", ep.NotFound, "url", string(req.RequestURI()), "func", ep.DestFunction, "addr", ep.HostClient.Addr, "host", string(req.URI().Host()))

	if err := ep.HostClient.Do(req, resp); err != nil {
		ctx.Logger().Printf("error when proxying the request: %s", err)
	}
	postprocessResponse(resp)
}

func prepareRequest(req *fasthttp.Request) {
	// do not proxy "Connection" header.
	req.Header.Del("Connection")
	// strip other unneeded headers.

	// alter other request params before sending them to upstream host
}

func postprocessResponse(resp *fasthttp.Response) {
	// do not proxy "Connection" header
	resp.Header.Del("Connection")

	// strip other unneeded headers

	// alter other response data if needed
}
