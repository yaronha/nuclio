package testlib

import (
	"github.com/nuclio/nuclio/cmd/dealer/app"
	"fmt"
	"testing"
	"github.com/nuclio/nuclio/pkg/dealer/jobs"
	"github.com/nuclio/logger"
	"strings"
	"strconv"
)

func NewTestContext(test *testing.T, dealer *app.Dealer, log logger.Logger)  TestContext {
	return TestContext{test:test, dealer:dealer, logger:log.GetChild("test")}
}

type TestContext struct {
	test     *testing.T
	dealer   *app.Dealer
	logger   logger.Logger
	unique   int
}

func (tc *TestContext) uniqueStr() string {
	tc.unique++
	return fmt.Sprintf("%04x", tc.unique)
}


func NewFunc(ctx *TestContext, name, version string) *functionBase {
	namespace := "nuclio"
	alias := ""
	if version == "" {
		version = "latest"
		alias = "latest"
	}
	return &functionBase{
		ctx:ctx, name:name, namespace:namespace, version:version, alias:alias,
		jobs: map[string]*jobs.BaseJob{},
		procAddresses: []procAddress{}}
}

type functionBase struct {
	ctx          *TestContext
	name         string
	namespace    string
	version      string
	alias        string
	jobs         map[string]*jobs.BaseJob
	gen          int
	desiredScale int
	lastScale    int
	procAddresses []procAddress
}

type procAddress struct {
	name, ip string
	port int
}


func (fn *functionBase) AddProc(name string, url string) {
	list := strings.Split(url, ":")
	proc := procAddress{name:name}
	if len(list) <= 1 {
		proc.ip = url
		proc.port = 8081
	} else {
		proc.ip = list[0]
		port, _ := strconv.Atoi(list[1])
		proc.port = port
	}
	fn.procAddresses = append(fn.procAddresses, proc)
	fn.desiredScale = len(fn.procAddresses)
	return
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

func (fn *functionBase) updateDeployment() (*jobs.DeploymentMessage, error) {
	triggers := []*jobs.BaseJob{}
	for _, job := range fn.jobs {
		triggers = append(triggers, job)
	}

	newDep := jobs.DeploymentSpec{BaseDeployment:jobs.BaseDeployment{
		Name:"dep-"+fn.name+"-"+fn.version, Namespace:fn.namespace,
		Function:fn.name,Version:fn.version, Alias:fn.alias,
		ExpectedProc:fn.desiredScale, FuncGen:fmt.Sprintf("%04x", fn.gen)},
		Triggers:triggers}

	dep, err := fn.ctx.dealer.Ctx.SubmitReq(&jobs.RequestMessage{
		Object: &newDep, Type: jobs.RequestTypeDeployUpdate})

	fn.ctx.logger.InfoWith("Updated dealer: ","dealer", dep)
	CheckErr(fn.ctx.test, err)

	return dep.(*jobs.DeploymentMessage), err
}

func (fn *functionBase) CreateDeployment() error {
	_, err := fn.updateDeployment()

	if err !=nil  {
		return err
	}

	return nil

}

func (fn *functionBase) UpdateDeployment(scale int) error {

	fn.desiredScale = scale
	dep, err := fn.updateDeployment()

	if err !=nil || scale == fn.lastScale {
		return err
	}

	waitMs(50)
	fmt.Println("SCALE:",scale, fn.lastScale)
	if scale > fn.lastScale {
		for {
			fn.lastScale++
			fn.updateProc(fn.lastScale-1, nil)
			if scale == fn.lastScale { break }
		}
	} else {
		for {
			fn.lastScale--
			name := dep.Processes[fn.lastScale]
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


func (fn *functionBase) updateProc(index int, jb map[string]jobs.JobShort) error {

	var procAddr procAddress
	if len(fn.procAddresses) <= index {
		procAddr.name = fn.name + "-" + fn.ctx.uniqueStr()
	} else {
		procAddr = fn.procAddresses[index]
	}
	msg := &jobs.ProcessMessage{
		BaseProcess: jobs.BaseProcess{Name:procAddr.name, Namespace:fn.namespace,
			Function:fn.name, Version:fn.version, Alias:fn.alias, IP: procAddr.ip, Port:procAddr.port}, Jobs: jb,
	}
	proc, err := fn.ctx.dealer.Ctx.SubmitReq(&jobs.RequestMessage{Object:msg, Type:jobs.RequestTypeProcUpdate})
	fn.ctx.logger.InfoWith("Updated proc: ","proc", proc)
	CheckErr(fn.ctx.test, err)
	return err
}


func (fn *functionBase) ProcInfo() *procsInfoStruct {
	info := procsInfoStruct{}
	info.ProcsTasks = map[string]procResults{}
	info.JobTasks = map[string]procResults{}

	strList := []string{}

	procList, err := fn.ctx.dealer.Ctx.SubmitReq(&jobs.RequestMessage{Name: "",
		Namespace: fn.namespace, Function:fn.name, Version:fn.version, Type: jobs.RequestTypeProcList})

	info.err = err
	CheckErr(fn.ctx.test, err)

	for _, proc := range procList.([]*jobs.ProcessMessage) {

		procSum := procResults{}


		tasks := ""
		//procSum = 0
		for jobName, job := range proc.Jobs {
			jobSum, ok := info.JobTasks[jobName]
			if !ok {
				jobSum = procResults{}
			}
			active:=0
			for _, task := range job.Tasks {
				if task.State == jobs.TaskStateRunning || task.State == jobs.TaskStateAlloc {
					jobSum.active++
					procSum.active++
				}
				if task.State == jobs.TaskStateStopping {
					jobSum.stopped++
					procSum.stopped++
				}
				jobSum.total++
				procSum.total++
			}

			tasks += fmt.Sprintf("%s(%d) ", jobName, active)
			info.JobTasks[jobName] = jobSum
		}

		str := fmt.Sprintf("%s-%d: %s", proc.Name, proc.State, tasks)
		strList = append(strList, str)

		info.ProcsTasks[proc.Name] = procSum

		totals := info.Totals
		totals.active += procSum.active
		totals.total += procSum.total
		totals.stopped += procSum.stopped
		info.Totals = totals
	}
	fn.ctx.logger.InfoWith("Process states: ", "procs", strList, "info", info)

	return &info
}


type procResults struct {
	total, active, stopped int
}

type procsInfoStruct struct {
	err           error
	Totals        procResults
	ProcsTasks    map[string]procResults
	JobTasks      map[string]procResults
}

func (pi procsInfoStruct) ProcTasksInRange(from, to int, active bool) bool {
	for _, p := range pi.ProcsTasks {
		if active {
			if p.active < from || p.active > to {
				return false
			}
		} else {
			if p.total < from || p.total > to {
				return false
			}
		}
	}
	return true
}