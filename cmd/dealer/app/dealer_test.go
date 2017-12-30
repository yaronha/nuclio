package app

import (
	"testing"
	"github.com/nuclio/nuclio-sdk"
	"github.com/nuclio/nuclio/pkg/zap"
	"github.com/pkg/errors"
	"github.com/nuclio/nuclio/pkg/dealer/jobs"
	"time"
)

type testContext struct {
	test     *testing.T
	dealer   *Dealer
	logger   nuclio.Logger
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
	err = fn.updateDep(2, "", &jobs.BaseJob{Name:"st1",TotalTasks:5})
	time.Sleep(time.Duration(50) * time.Millisecond)

	err = fn.updateProc("p1", nil )
	time.Sleep(time.Duration(50) * time.Millisecond)

	err = fn.updateProc("p2", nil )
	time.Sleep(time.Duration(50) * time.Millisecond)
	ctx.logger.DebugWith("proc", "proc", dealer.Processes["p1.default"].GetProcessState())

	err = fn.updateDep(2, "11", &jobs.BaseJob{Name:"st1",TotalTasks:5, Disable:true})
	time.Sleep(time.Duration(50) * time.Millisecond)

	err = fn.updateProc("p3", nil )
	time.Sleep(time.Duration(50) * time.Millisecond)


	fn.procSum("p1","p2", "p3")
	err = fn.updateDep(2, "12", &jobs.BaseJob{Name:"st1",TotalTasks:5})
	time.Sleep(time.Duration(50) * time.Millisecond)

	dealer.removeProcess("p2", "default")
	time.Sleep(time.Duration(50) * time.Millisecond)
	fn.procSum("p1", "p3")

}

func newFunc(ctx *testContext, name, version string) *functionBase {
	namespace := "default"
	alias := ""
	if version == "" {
		version = "latest"
		alias = "latest"
	}
	return &functionBase{ctx:ctx, name:name, namespace:namespace, version:version, alias:alias}
}

type functionBase struct {
	ctx   *testContext
	name, namespace, version, alias string
}

func (fn *functionBase) procSum(procs ...string) {
	strList := []interface{}{}
	for _, proc := range procs{
		str := fn.ctx.dealer.Processes[jobs.ProcessKey(proc,fn.namespace)].AsString()
		strList = append(strList, str)
	}
	fn.ctx.logger.InfoWith("Process states: ", "procs", strList)
}

func (fn *functionBase) updateDep(procs int, funcgen string, triggers ...*jobs.BaseJob) error {
	newDep := jobs.DeploymentSpec{BaseDeployment:jobs.BaseDeployment{
		Name:"dep-"+fn.name+"-"+fn.version, Namespace:fn.namespace,
		Function:fn.name,Version:fn.version, Alias:fn.alias, ExpectedProc:procs, FuncGen:funcgen},
		Triggers:triggers}
	dep, err := fn.ctx.dealer.Ctx.SubmitReq(&jobs.RequestMessage{
		Object: &newDep, Type: jobs.RequestTypeDeployUpdate})
	fn.ctx.logger.InfoWith("Updated dealer: ","dealer", dep)
	checkErr(fn.ctx.test, err)
	return err
}

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