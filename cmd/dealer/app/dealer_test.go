package app

import (
	"testing"
	"github.com/nuclio/nuclio-sdk"
	"github.com/nuclio/nuclio/pkg/zap"
	"github.com/pkg/errors"
	"github.com/nuclio/nuclio/pkg/dealer/jobs"
	"time"
	"fmt"
)

type testContext struct {
	test     *testing.T
	dealer   *Dealer
	logger   nuclio.Logger
	unique   int
}

func (tc *testContext) uniqueStr() string {
	tc.unique++
	return fmt.Sprintf("%04x", tc.unique)
}

func TestDealer_Start(t *testing.T) {
	logger, _ := createLogger(false)

	dealer, err := NewDealer(logger, &jobs.ManagerContextConfig{
		DisablePush: true,
		StorePath:   "",
	})
	checkErr(t, err)
	ctx := testContext{test:t, dealer:dealer, logger:logger.GetChild("test")}

	dealer.Start()

	fn := newFunc(&ctx, "fn1", "")
	fn.setJob("j1", &jobs.BaseJob{TotalTasks:5})
	fn.setJob("j2", &jobs.BaseJob{TotalTasks:7})

	fn.updateDep(3)
	fn.procSum()

	fn.setJob("j1", &jobs.BaseJob{TotalTasks:5, Disable:true})
	fn.updateDep(3)
	fn.procSum()
	fn.updateDep(1)

	fn.procSum()
	fn.updateDep(2)
	fn.procSum()

}

func newFunc(ctx *testContext, name, version string) *functionBase {
	namespace := "default"
	alias := ""
	if version == "" {
		version = "latest"
		alias = "latest"
	}
	return &functionBase{ctx:ctx, name:name, namespace:namespace, version:version, alias:alias, jobs: map[string]*jobs.BaseJob{}}
}

type functionBase struct {
	ctx        *testContext
	name, namespace, version, alias string
	jobs       map[string]*jobs.BaseJob
	gen        int
	lastScale  int
}

func waitMs(ms int) { time.Sleep(time.Duration(ms) * time.Millisecond) }

func (fn *functionBase) setJob(name string, job *jobs.BaseJob) {
	fn.gen++
	job.Name = name
	fn.jobs[name] = job
	return
}


func (fn *functionBase) procSum(procs ...string) {
	strList := []interface{}{}
	for _, proc := range fn.ctx.dealer.Processes {
		str := proc.AsString()
		strList = append(strList, str)
	}
	fn.ctx.logger.InfoWith("Process states: ", "procs", strList)
}

func (fn *functionBase) updateDep(scale int) error {
	triggers := []*jobs.BaseJob{}
	for _, job := range fn.jobs {
		triggers = append(triggers, job)
	}

	newDep := jobs.DeploymentSpec{BaseDeployment:jobs.BaseDeployment{
		Name:"dep-"+fn.name+"-"+fn.version, Namespace:fn.namespace,
		Function:fn.name,Version:fn.version, Alias:fn.alias,
		ExpectedProc:scale, FuncGen:fmt.Sprintf("%04x", fn.gen)},
		Triggers:triggers}

	dep, err := fn.ctx.dealer.Ctx.SubmitReq(&jobs.RequestMessage{
		Object: &newDep, Type: jobs.RequestTypeDeployUpdate})

	fn.ctx.logger.InfoWith("Updated dealer: ","dealer", dep)
	checkErr(fn.ctx.test, err)

	if err !=nil || scale == fn.lastScale {
		return err
	}

	waitMs(50)
	if scale > fn.lastScale {
		for {
			fn.lastScale++
			fn.updateProc(fn.name + "-" + fn.ctx.uniqueStr(), nil)
			if scale == fn.lastScale { break }
		}
	} else {
		for {
			fn.lastScale--
			name := dep.(*jobs.DeploymentMessage).Processes[fn.lastScale]
			_, err := fn.ctx.dealer.Ctx.SubmitReq(&jobs.RequestMessage{Namespace:fn.namespace, Name:name, Type:jobs.RequestTypeProcDel})
			fn.ctx.logger.DebugWith("Del proc: ","proc", name, "err", err)
			if scale == fn.lastScale { break }
		}
	}

	waitMs(50)
	return err
}



//dep, err := fn.ctx.dealer.Ctx.SubmitReq(&jobs.RequestMessage{
//Namespace:fn.namespace, Function:fn.name, Version:fn.version, Type: jobs.RequestTypeDeployGet})
//fn.ctx.logger.InfoWith("Updated dealer: ","dealer", dep, "err", err)


func (fn *functionBase) updateProc(name string, jb map[string]jobs.JobShort) error {
	msg := &jobs.ProcessMessage{
		BaseProcess: jobs.BaseProcess{Name:name, Namespace:fn.namespace,
			Function:fn.name, Version:fn.version, Alias:fn.alias}, Jobs: jb,
	}
	proc, err := fn.ctx.dealer.Ctx.SubmitReq(&jobs.RequestMessage{Object:msg, Type:jobs.RequestTypeProcUpdate})
	fn.ctx.logger.InfoWith("Updated proc: ","proc", proc)
	checkErr(fn.ctx.test, err)
	return err
}

func checkErr(t *testing.T, err error) {
	if err != nil {
		t.Error(err)
	}
}

func createLogger(verbose bool) (nuclio.Logger, error) {
	var loggerLevel nucliozap.Level

	if verbose {
		loggerLevel = nucliozap.DebugLevel
	} else {
		loggerLevel = nucliozap.InfoLevel
	}

	logger, err := nucliozap.NewNuclioZapCmd("dealer", loggerLevel)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create logger")
	}

	return logger, nil

}