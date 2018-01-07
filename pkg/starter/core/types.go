package core

import (
	"github.com/nuclio/nuclio/pkg/functionconfig"
	"time"
	"fmt"
	"math/rand"
)


type RequestType int

const (
	RequestTypeUnknown RequestType = iota

	RequestTypeUpdateFunction
	RequestTypeDelFunction
	RequestTypeUpdateEndPoints
	RequestTypeLookup
)

type AsyncRequests struct {
	Type       RequestType
	Data       interface{}
	ErrChan    chan error
}

func getFuncKey(namespace, name, version string) string {
	return namespace + "." + name + "." + version
}

type FunctionBase struct {
	Namespace     string
	CRName        string
	Function      string
	Version       string
	Ingresses     []functionconfig.Ingress
	CanScaledown  bool    // can this function be scaled down?  (if it only has HTTP triggers)
	Disabled      bool
	Gen           string

}

type FunctionEndPoints struct {
	Namespace     string
	Name          string
	Version       string
	APIPort       int
	ControlPort   int
	IPs           []string

}

type FunctionRecord struct {
	FunctionBase

	lastEventCnt  int
	lastStats     time.Time
	apiPort       int
	controlPort   int
	endPoints     []string
	ready         bool
	requests      []*LookupRequest

}

func (fn *FunctionRecord) IsReady() bool {
	return !fn.Disabled && (len(fn.endPoints) > 0)
}

func (fn *FunctionRecord) releaseRequests() {

	for _, request := range fn.requests {
		request.ReturnChan <- &LookupResponse{DestURL: fn.getFunctionURL(), DestFunction: fn.Function + ":" + fn.Version}
	}

	fn.requests = []*LookupRequest{}
}

func (fn *FunctionRecord) getFunctionURL() string {
	idx := rand.Intn(len(fn.endPoints))
	return fmt.Sprintf("%s:%d", fn.endPoints[idx], fn.apiPort)
}

type LookupRequest struct {
	SourceIP    string
	SourcePort  int
	Host        string
	Path        string
	ReturnChan  chan *LookupResponse
}

type LookupResponse struct {
	NotFound      bool
	DestURL       string
	DestFunction  string
}

