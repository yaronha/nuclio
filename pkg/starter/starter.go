package starter

import (
	"github.com/nuclio/nuclio-sdk"
	"github.com/nuclio/nuclio/pkg/starter/core"
	"github.com/nuclio/nuclio/pkg/starter/kube"
	"github.com/pkg/errors"
	"time"
	"github.com/nuclio/nuclio/pkg/platform/kube/functioncr"
)

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
	logger           nuclio.Logger
	config           *StarterConfig
	RequestsChannel  chan *core.AsyncRequests
	funcDB           *core.FuncDirectory
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


	time.Sleep(time.Second)

	retChan := make(chan *core.LookupResponse)
	req := &core.LookupRequest{ Host:"kuku", Path:"/encrypt/latest/xx", ReturnChan: retChan}
	s.RequestsChannel <- &core.AsyncRequests{ Type: core.RequestTypeLookup, Data: req}

	resp := <- retChan
	s.logger.DebugWith("Lookup resp", "notfound", resp.NotFound, "url", resp.DestURL, "func", resp.DestFunction)

	return nil

}

