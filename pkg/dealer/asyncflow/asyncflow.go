package asyncflow

import (
	"time"
	"github.com/nuclio/logger"
)

func NewWorkflowTask(logger logger.Logger, tasksChannel *chan *AsyncWorkflowTask, task AsyncWorkflowTask) *AsyncWorkflowTask {
	task.logger = logger
	task.asyncTasksChannel = tasksChannel
	task.quit = make(chan bool)
	if task.TimeoutSec == 0 {
		task.TimeoutSec = 60
	}
	return &task
}

type AsyncWorkflowTask struct {
	logger            logger.Logger
	asyncTasksChannel *chan *AsyncWorkflowTask
	Name              string
	startedOn         time.Time
	resp              interface{}
	respErr           error
	quit              chan bool
	gotTimeout        bool
	processed         bool
	asyncCB           *AsyncCB
	cbIndex           int

	TimeoutSec int
	Data       interface{}
	OnTimeout  func(*AsyncWorkflowTask)
	OnComplete func(*AsyncWorkflowTask)
}

func (wt *AsyncWorkflowTask) start() {

	wt.startedOn = time.Now()
	go func() {
		for {
			select {
			case <-wt.quit:
				wt.quit <- true
				return
			case <-time.After(time.Second * time.Duration(wt.TimeoutSec)):
				wt.gotTimeout = true
				*wt.asyncTasksChannel <- wt
				return
			}
		}
	}()
}

func (wt *AsyncWorkflowTask) CallOnTimeout() {
	if !wt.processed {
		wt.OnTimeout(wt)
		wt.processed = true
	}

	if wt.asyncCB != nil {
		wt.asyncCB.remove(wt.cbIndex)
		wt.asyncCB = nil
	}
}

func (wt *AsyncWorkflowTask) CallOnComplete() {
	if !wt.processed {
		wt.OnComplete(wt)
		wt.processed = true
	}
}

func (wt *AsyncWorkflowTask) Stop() {
	wt.quit <- true
	<-wt.quit
}

func (wt *AsyncWorkflowTask) complete(resp interface{}, err error) {
	// Stop wait for timeout for thread safety
	if wt.processed {
		return
	}

	wt.Stop()
	wt.logger.DebugWith("Async task completed", "name", wt.Name, "duration", time.Since(wt.startedOn).String(),
		"data", wt.Data, "resp", resp, "err", err)
	wt.resp = resp
	wt.respErr = err
	if wt.asyncCB != nil {
		wt.asyncCB.remove(wt.cbIndex)
		wt.asyncCB = nil
	}
	*wt.asyncTasksChannel <- wt
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

func NewAsyncCB() *AsyncCB {
	return &AsyncCB{}
}

type AsyncCB struct {
	list []*AsyncWorkflowTask
}

func (a *AsyncCB) AddTask(task *AsyncWorkflowTask) {
	a.list = append(a.list, task)
	task.asyncCB = a
	task.cbIndex = len(a.list) - 1
	task.start()
}

func (a *AsyncCB) Call(resp interface{}, err error) {
	for _, task := range a.list {
		task.complete(resp, err)
	}
	a.list = []*AsyncWorkflowTask{}
}

func (a *AsyncCB) remove(idx int) {
	a.list = append(a.list[:idx], a.list[idx+1:]...)
}
