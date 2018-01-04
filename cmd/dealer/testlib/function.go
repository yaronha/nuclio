package testlib

import (
	"github.com/nuclio/nuclio-sdk"
	"github.com/nuclio/nuclio/cmd/dealer/app"
	"fmt"
	"testing"
	"github.com/nuclio/nuclio/pkg/dealer/jobs"
)

func NewTestContext(test *testing.T, dealer *app.Dealer, logger nuclio.Logger)  TestContext {
	return TestContext{test:test, dealer:dealer, logger:logger.GetChild("test")}
}

type TestContext struct {
	test     *testing.T
	dealer   *app.Dealer
	logger   nuclio.Logger
	unique   int
}

func (tc *TestContext) uniqueStr() string {
	tc.unique++
	return fmt.Sprintf("%04x", tc.unique)
}


func NewFunc(ctx *TestContext, name, version string) *functionBase {
	namespace := "default"
	alias := ""
	if version == "" {
		version = "latest"
		alias = "latest"
	}
	return &functionBase{ctx:ctx, name:name, namespace:namespace, version:version, alias:alias, jobs: map[string]*jobs.BaseJob{}}
}

type functionBase struct {
	ctx        *TestContext
	name       string
	namespace  string
	version    string
	alias      string
	jobs       map[string]*jobs.BaseJob
	gen        int
	lastScale  int
}


func (fn *functionBase) SetJob(name string, job *jobs.BaseJob) {
	fn.gen++
	job.Name = name
	fn.jobs[name] = job
	return
}

func (fn *functionBase) RemoveJob(name string) {
	delete(fn.jobs, name)
}


func (fn *functionBase) ProcSum() {
	strList := []string{}
	var totalActive, totalStop, procSum, procAvg, procMax int
	procMin := 9999

	procList, err := fn.ctx.dealer.Ctx.SubmitReq(&jobs.RequestMessage{Name: "",
		Namespace: fn.namespace, Function:fn.name, Version:fn.version, Type: jobs.RequestTypeProcList})

	CheckErr(fn.ctx.test, err)

	for _, proc := range procList.([]*jobs.ProcessMessage) {

		tasks := ""
		procSum = 0
		for jobName, job := range proc.Jobs {
			active:=0
			for _, task := range job.Tasks {
				if task.State == jobs.TaskStateRunning || task.State == jobs.TaskStateAlloc {
					active++
				}
				if task.State == jobs.TaskStateStopping {
					totalStop++
				}
			}
			procSum += active

			tasks += fmt.Sprintf("%s(%d) ", jobName, active)
		}

		if procSum > procMax { procMax = procSum }
		if procSum < procMin { procMin = procSum }
		totalActive += procSum
		str := fmt.Sprintf("%s-%d: %s", proc.Name, proc.State, tasks)
		strList = append(strList, str)
	}
	procAvg = procSum / len(procList.([]*jobs.ProcessMessage))
	fn.ctx.logger.InfoWith("Process states: ", "procs", strList)
}

func (fn *functionBase) UpdateDeployment(scale int) error {
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
	CheckErr(fn.ctx.test, err)

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
	CheckErr(fn.ctx.test, err)
	return err
}
