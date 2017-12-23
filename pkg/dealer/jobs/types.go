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
	"github.com/nuclio/nuclio-sdk"
	"github.com/nuclio/nuclio/pkg/dealer/client"
	"time"
)

type ManagerContext struct {
	Logger            nuclio.Logger
	RequestsChannel   chan *RequestMessage
	ProcRespChannel   chan *client.Response
	AsyncTasksChannel chan *AsyncWorkflowTask
	Client            *client.AsyncClient
	JobStore          JobStore
	DisablePush       bool
}

type ManagerContextConfig struct {
	DisablePush bool
	StorePath   string
}

func NewManagerContext(logger nuclio.Logger, asyncClient *client.AsyncClient, config *ManagerContextConfig) *ManagerContext {
	newContext := ManagerContext{
		Logger:            logger.GetChild("jobMng").(nuclio.Logger),
		RequestsChannel:   make(chan *RequestMessage, 100),
		ProcRespChannel:   make(chan *client.Response, 100),
		AsyncTasksChannel: make(chan *AsyncWorkflowTask, 100),
		Client:            asyncClient,
		DisablePush:       config.DisablePush,
	}

	newContext.JobStore = NewJobFileStore(config.StorePath, logger)
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

func (mc *ManagerContext) SaveJobs(jobs map[string]*Job) {
	if len(jobs) == 0 {
		return
	}

	mc.Logger.DebugWith("Saving Jobs", "jobs", len(jobs))
	for _, job := range jobs {
		err := mc.JobStore.SaveJob(job)
		if err != nil {
			mc.Logger.ErrorWith("Error Saving Job", "jobs", job.Name, "err", err)
		}
	}
}

func (mc *ManagerContext) DeleteJobRecords(jobs map[string]*Job) {
	if len(jobs) == 0 {
		return
	}

	mc.Logger.DebugWith("Deleting Jobs", "jobs", jobs)
	// TODO: delete jobs state from persistent storage
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
	Type       RequestType
	Object     interface{}
	ReturnChan chan *RespChanType
}

func (mc *ManagerContext) NewWorkflowTask(task AsyncWorkflowTask) *AsyncWorkflowTask {
	task.mc = mc
	task.quit = make(chan bool)
	if task.TimeoutSec == 0 {
		task.TimeoutSec = 60
	}
	return &task
}

type AsyncWorkflowTask struct {
	mc         *ManagerContext
	Name       string
	startedOn  time.Time
	resp       interface{}
	respErr    error
	quit       chan bool
	gotTimeout bool
	stopped    bool

	TimeoutSec int
	Data       interface{}
	OnTimeout  func(*AsyncWorkflowTask)
	OnComplete func(*AsyncWorkflowTask)
}

func (wt *AsyncWorkflowTask) Start() {

	wt.startedOn = time.Now()
	go func() {
		for {
			select {
			case <-wt.quit:
				wt.quit <- true
				return
			case <-time.After(time.Second * time.Duration(wt.TimeoutSec)):
				wt.mc.Logger.WarnWith("Async task timed-out", "name", wt.Name, "data", wt.Data)
				wt.gotTimeout = true
				wt.stopped = true
				wt.mc.AsyncTasksChannel <- wt
				return
			}
		}
	}()
}

func (wt *AsyncWorkflowTask) Stop() {
	wt.quit <- true
	<-wt.quit
	wt.stopped = true
}

func (wt *AsyncWorkflowTask) Complete(resp interface{}, err error) {
	// Stop wait for timeout for thread safety, TODO: stopped safety
	if wt.stopped {
		return
	}
	wt.Stop()
	wt.mc.Logger.DebugWith("Async task completed", "name", wt.Name, "duration", time.Since(wt.startedOn).String(),
		"data", wt.Data, "resp", resp, "err", err)
	wt.resp = resp
	wt.respErr = err
	wt.stopped = true
	wt.mc.AsyncTasksChannel <- wt
}

func (wt *AsyncWorkflowTask) GetResp() interface{} {
	return wt.resp
}

func (wt *AsyncWorkflowTask) GetErr() error {
	return wt.respErr
}

func (wt *AsyncWorkflowTask) IsTimeout() bool {
	return wt.gotTimeout
}
