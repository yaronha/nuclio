/*
Copyright 2017 The Nuclio Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package jobs

import (
	"time"
	"net/http"
	"github.com/nuclio/nuclio/pkg/dealer/client"
)

type ProcessUpdateMessage2 struct {
	Name          string
	Namespace     string
	IP            string
	Metrics       map[string]int
	State         ProcessState
	LastUpdate    time.Time
	job           string
	CurrentTasks  []Task
}


func (p *ProcessUpdateMessage2) Render(w http.ResponseWriter, r *http.Request) error {
	return nil
}

type ProcessUpdateResp struct {
	Name          string
	Namespace     string
	IP            string
	job           string
	DesiredState  ProcessState
	Tasks         []Task
}

type ManagerContext struct {
	RequestsChannel  chan *RequestMessage
	OutChannel       chan *client.ChanRequest
	ProcRespChannel  chan *client.Response
	Client           *client.AsyncClient
}

func (mc *ManagerContext) SubmitReq(request *RequestMessage) (interface{}, error) {
	respChan := make(chan *RespChanType)
	request.ReturnChan = respChan
	mc.RequestsChannel <- request
	resp := <- respChan
	return resp.Object, resp.Err
	return nil, nil
}



type RequestType int

const (
	RequestTypeUnknown      RequestType = iota

	RequestTypeJobGet
	RequestTypeJobDel
	RequestTypeJobList
	RequestTypeJobCreate
	RequestTypeJobUpdate

	RequestTypeProcGet
	RequestTypeProcDel
	RequestTypeProcList
	RequestTypeProcCreate
	RequestTypeProcUpdate

	RequestTypeDeployUpdate
	RequestTypeDeployRemove
	RequestTypeDeployList

)

type RespChanType struct {
	Err error
	Object interface{}
}

type RequestMessage struct {
	Name string
	Namespace string
	Type RequestType
	Object interface{}
	ReturnChan chan *RespChanType
}
