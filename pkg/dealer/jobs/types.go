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
	"github.com/nuclio/nuclio/pkg/dealer/asyncflow"
	"github.com/nuclio/nuclio/pkg/dealer/client"
	"github.com/nuclio/logger"
)

type ManagerContext struct {
	Logger            logger.Logger
	RequestsChannel   chan *RequestMessage
	ProcRespChannel   chan *client.Response
	AsyncTasksChannel chan *asyncflow.AsyncWorkflowTask
	Client            *client.AsyncClient
	JobStore          JobStore
	DisablePush       bool
}

type ManagerContextConfig struct {
	DisablePush bool
	StorePath   string
}

func NewManagerContext(log logger.Logger, asyncClient *client.AsyncClient, config *ManagerContextConfig) *ManagerContext {
	newContext := ManagerContext{
		Logger:            log.GetChild("jobMng").(logger.Logger),
		RequestsChannel:   make(chan *RequestMessage, 100),
		ProcRespChannel:   make(chan *client.Response, 100),
		AsyncTasksChannel: make(chan *asyncflow.AsyncWorkflowTask, 100),
		Client:            asyncClient,
		DisablePush:       config.DisablePush,
	}

	newContext.JobStore = NewJobFileStore(config.StorePath, log)
	return &newContext
}

func (mc *ManagerContext) SubmitReq(request *RequestMessage) (interface{}, error) {
	respChan := make(chan *RespChanType)
	request.ReturnChan = respChan
	mc.RequestsChannel <- request
	resp := <-respChan
	return resp.Object, resp.Err
	return nil, nil
}

func (mc *ManagerContext) SaveJobs(jobs []*Job) {
	if len(jobs) == 0 {
		return
	}

	mc.Logger.DebugWith("Saving Jobs", "jobs", len(jobs))
	for _, job := range jobs {
		err := mc.JobStore.SaveJob(job)
		if err != nil {
			mc.Logger.ErrorWith("Error Saving Job", "job", job.Name, "err", err)
		}
	}
}

func (mc *ManagerContext) DeleteJobRecord(job *Job) {

	mc.Logger.DebugWith("Deleting Job", "job", job)
	err := mc.JobStore.DelJob(job)
	if err != nil {
		mc.Logger.ErrorWith("Error Deleting Job", "job", job.Name, "err", err)
	}
}

func (mc *ManagerContext) NewWorkflowTask(task asyncflow.AsyncWorkflowTask) *asyncflow.AsyncWorkflowTask {
	return asyncflow.NewWorkflowTask(mc.Logger, &mc.AsyncTasksChannel, task)
}

type RequestType int

const (
	RequestTypeUnknown RequestType = iota

	RequestTypeJobGet
	RequestTypeJobDel
	RequestTypeJobList
	RequestTypeJobCreate
	RequestTypeJobUpdate

	RequestTypeProcGet
	RequestTypeProcDel
	RequestTypeProcList
	RequestTypeProcUpdateState
	RequestTypeProcUpdate
	RequestTypeProcHealth

	RequestTypeDeployGet
	RequestTypeDeployUpdate
	RequestTypeDeployRemove
	RequestTypeDeployList
)

type RespChanType struct {
	Err    error
	Object interface{}
}

type RequestMessage struct {
	Namespace  string
	Function   string // for jobs
	Name       string
	Version    string
	Type       RequestType
	Object     interface{}
	ReturnChan chan *RespChanType
}
