package jobs

import (
	"fmt"
	"github.com/pkg/errors"
	"net/http"
)

type Deployment struct {
	dm        *DeploymentMap
	Namespace string `json:"namespace"`
	Function  string `json:"function"`
	Name      string `json:"name"`
	Version   string `json:"version,omitempty"`
	Alias     string `json:"alias,omitempty"`

	Triggers     []*Trigger `json:"triggers,omitempty"`
	ExpectedProc int        `json:"expectedProc,omitempty"`
	procs        map[string]*Process
	jobs         map[string]*Job
}

type DeploymentMessage struct {
	Deployment
	Processes []string
	Jobs      []string
}

func (d *DeploymentMessage) Bind(r *http.Request) error {
	return nil
}

func (d *DeploymentMessage) Render(w http.ResponseWriter, r *http.Request) error {
	return nil
}

type Trigger struct {
	Name              string      `json:"name"`
	TotalTasks        int         `json:"totalTasks"`
	MaxTaskAllocation int         `json:"maxTaskAllocation,omitempty"`
	MinProcesses      int         `json:"minProcesses,omitempty"`
	MaxProcesses      int         `json:"maxProcesses,omitempty"`
	Metadata          interface{} `json:"metadata,omitempty"`
	IsMultiVersion    bool        `json:"isMultiVersion,omitempty"`
}

func (d *Deployment) AddProcess(proc *Process) {
	d.procs[proc.Name] = proc
	proc.deployment = d
}

// handle process removal
func (d *Deployment) RemoveProcess(proc *Process) error {

	err := proc.Remove()
	if err != nil {
		return errors.Wrap(err, "Failed to remove process")
	}

	d.procs[proc.Name].Remove()
	delete(d.procs, proc.Name)

	return d.Rebalance()
}

func (d *Deployment) GetProcs() map[string]*Process {
	return d.procs
}

func (d *Deployment) GetJobs() map[string]*Job {
	return d.jobs
}

func (d *Deployment) GetDeploymentState() *DeploymentMessage {
	dep := DeploymentMessage{Deployment: *d}
	dep.Processes = []string{}
	dep.Jobs = []string{}

	for name, _ := range d.procs {
		dep.Processes = append(dep.Processes, name)
	}
	for name, _ := range d.jobs {
		dep.Jobs = append(dep.Jobs, name)
	}
	return &dep

}

func (d *Deployment) SumTasks() int {
	tasks := 0
	for _, job := range d.jobs {
		tasks += job.TotalTasks
	}
	return tasks
}

// allocate available tasks to process
func (d *Deployment) AllocateTasks(proc *Process) error {

	totalTasks := d.SumTasks()
	tasksPerProc := totalTasks / d.ExpectedProc
	taskReminder := totalTasks - d.ExpectedProc*tasksPerProc
	var totalAsigned, procTasks, aboveMin int

	for _, p := range d.procs {
		procTasks = len(p.GetTasks(true))
		totalAsigned += procTasks
		if procTasks > tasksPerProc {
			aboveMin += 1
		}
	}

	totalUnallocated := totalTasks - totalAsigned
	if totalUnallocated < 0 {
		return fmt.Errorf("Assigned Tasks (%d) greater than Total tasks (%d)", totalAsigned, totalTasks)
	}
	if totalUnallocated == 0 {
		return nil
	}

	alloc := tasksPerProc
	if aboveMin < taskReminder {
		alloc += 1
	}

	newAlloc := alloc - len(proc.GetTasks(true))
	if totalUnallocated <= newAlloc {
		newAlloc = totalUnallocated
	}

	_, err := d.addTasks2Proc(proc, newAlloc, totalTasks)
	if err != nil {
		return errors.Wrap(err, "Failed to add tasks")
	}

	return err
}

func (d *Deployment) Rebalance() error {

	if d.ExpectedProc == 0 {
		return nil
	}

	totalTasks := d.SumTasks()
	tasksPerProc := totalTasks / d.ExpectedProc
	taskReminder := totalTasks - d.ExpectedProc*tasksPerProc

	var procTasks, missingPlus1, extraPlus1 int
	var tasksUnder, tasksEqual, tasksPlus1, tasksOver []*Process

	for _, p := range d.procs {
		if !p.removingTasks {
			procTasks = len(p.GetTasks(true))
			switch {
			case procTasks < tasksPerProc:
				tasksUnder = append(tasksUnder, p)
			case procTasks == tasksPerProc:
				tasksEqual = append(tasksEqual, p)
			case procTasks == tasksPerProc+1:
				tasksPlus1 = append(tasksPlus1, p)
			default:
				tasksOver = append(tasksOver, p)
			}
		}
	}

	d.dm.logger.DebugWith("Rebalance", "deployment", d.Name, "expProcs", d.ExpectedProc, "procs", len(d.procs),
		"totTasks", totalTasks, "under", len(tasksUnder), "eq", len(tasksEqual), "plus1", len(tasksPlus1), "over", len(tasksOver))

	// desired state is: N with tasksPerProc+1, newnum-N with tasksPerProc (N=taskReminder)
	//   and must not go over MaxAllocation per Job

	missingPlus1 = taskReminder - len(tasksPlus1)
	if missingPlus1 < 0 {
		extraPlus1 = -missingPlus1
		missingPlus1 = 0
	}

	// TODO: StopN balanced across jobs
	for _, p := range tasksOver {
		tasks := p.GetTasks(true)
		if missingPlus1 > 0 {
			p.StopNTasks(len(tasks) - tasksPerProc - 1)
			missingPlus1 -= 1
		} else {
			p.StopNTasks(len(tasks) - tasksPerProc)
		}
		p.removingTasks = true
		_ = p.PushUpdates()
	}

	for _, p := range tasksPlus1 {
		if extraPlus1 > 0 {
			p.StopNTasks(1)
			p.removingTasks = true
			_ = p.PushUpdates()
			extraPlus1 -= 1
		} else {
			break
		}
	}

	tasksToAdd := append(tasksUnder, tasksEqual...)
	for _, p := range tasksToAdd {
		desired := tasksPerProc
		usedPlus1 := false
		if missingPlus1 > 0 {
			desired += 1
			usedPlus1 = true
		}

		newAlloc := desired - len(p.GetTasks(true))
		d.dm.logger.DebugWith("Rebalance - add tasks to proc", "proc", p.AsString(), "alloc", newAlloc, "missP1", missingPlus1, "remove", p.removingTasks)
		added, err := d.addTasks2Proc(p, newAlloc, totalTasks)
		if err != nil {
			return errors.Wrap(err, "Failed to add tasks")
		}
		if added < newAlloc {
			usedPlus1 = false
		}
		if usedPlus1 {
			missingPlus1 -= 1
		}
		_ = p.PushUpdates()
	}

	return nil
}

func (d *Deployment) addTasks2Proc(proc *Process, toAdd, totalTasks int) (int, error) {
	if toAdd == 0 {
		return 0, nil
	}
	type jobRec struct {
		job  *Job
		from int
	}
	jobList := []jobRec{}
	added := 0

	// First pass, give each job more tasks based on its share
	for _, j := range d.jobs {
		rec := jobRec{job: j}
		share := int(float64(toAdd*j.TotalTasks)/float64(totalTasks) + 0.5)
		maxAllocated := false

		// Make sure task allocation doesnt exceed job.MaxTaskAllocation (per process)
		if j.MaxTaskAllocation > 0 {
			d.dm.logger.DebugWith("addTasks2Proc - MaxAlloc", "proc", proc.AsString(), "share", share, "MaxTask", j.MaxTaskAllocation)
			if tasksLen := proc.GetJobTasksLen(j.Name, true); share+tasksLen > j.MaxTaskAllocation {
				share = j.MaxTaskAllocation - tasksLen
				maxAllocated = true
			}
		}

		toAlloc := j.findUnallocTask(share, &rec.from)
		toAllocLen := len(toAlloc)

		if toAllocLen > 0 {
			proc.AddTasks(toAlloc)
			added += toAllocLen
			if added >= toAdd {
				return added, nil
			}
		}

		// if toAlloc < share it means this job is fully allocated and can be skipped in next round (if didnt get to Max)
		// Prepend, so last job will go first in next allocation round
		if share == toAllocLen && !maxAllocated {
			jobList = append([]jobRec{rec}, jobList...)
		}
	}

	// 2nd pass, distribute the reminder
	for {
		newList := []jobRec{}
		for _, rec := range jobList {
			toAlloc := rec.job.findUnallocTask(1, &rec.from)

			if len(toAlloc) > 0 {
				proc.AddTasks(toAlloc)
				added += 1
				if added >= toAdd {
					return added, nil
				}
				newList = append(newList, rec)
			}
		}
		if len(newList) == 0 {
			return added, nil
		}
		jobList = newList
	}

	return added, nil
}

// read/update jobs from deployment
func (d *Deployment) updateJobs() error {
	for _, rjob := range d.Triggers {

		_, ok := d.jobs[rjob.Name]
		if !ok {
			// if this deployment doesnt contain the Job, create and add one
			newJob := &Job{Name: rjob.Name, Namespace: d.Namespace,
				Function: d.Function, Version: d.Version,
				TotalTasks: rjob.TotalTasks, MaxTaskAllocation: rjob.MaxTaskAllocation, Metadata: rjob.Metadata,
			}
			job, err := NewJob(d.dm.ctx, newJob)
			job.NeedToSave()

			if err != nil {
				d.dm.logger.ErrorWith("Failed to create a job", "deploy", d.Name, "job", rjob.Name, "err", err)
			}

			d.jobs[rjob.Name] = job
			d.dm.logger.DebugWith("Added new job to function", "function", d.Name, "job", rjob.Name, "tasks", rjob.TotalTasks)
		}

	}

	d.dm.ctx.SaveJobs(d.jobs)

	// balance tasks across processes (note proc list may still be empty at this point)
	err := d.Rebalance()
	if err != nil {
		d.dm.logger.ErrorWith("Failed to rebalance in updateJobs", "deploy", d.Name, "err", err)
		return err
	}

	// TODO: remove, just for ref
	//for _, proc := range d.procs {
	//	err := d.AllocateTasks(proc)
	//	if err != nil {
	//		d.dm.logger.ErrorWith("Failed to allocate jobtasks to proc", "deploy", d.Name, "proc", proc.Name, "err", err)
	//	}
	//}

	d.dm.logger.DebugWith("updateDeployJobs", "jobs", d.jobs)
	return nil
}

// add job while the deployment is working
func (d *Deployment) AddJob(rjob *Job) error {

	_, ok := d.jobs[rjob.Name]
	if !ok {
		// if this deployment doesnt contain the Job, create and add one
		newJob := &Job{Name: rjob.Name, Namespace: d.Namespace,
			Function: d.Function, Version: d.Version,
			TotalTasks: rjob.TotalTasks, MaxTaskAllocation: rjob.MaxTaskAllocation, Metadata: rjob.Metadata,
		}
		job, err := NewJob(d.dm.ctx, newJob)
		job.postDeployment = true
		job.NeedToSave()
		if err != nil {
			d.dm.logger.ErrorWith("Failed to create a job", "deploy", d.Name, "job", rjob.Name, "err", err)
		}

		d.jobs[rjob.Name] = job
		d.dm.ctx.SaveJobs(d.jobs)

		d.dm.logger.DebugWith("Added new job to function", "function", d.Name, "job", rjob.Name, "tasks", rjob.TotalTasks)
		err = d.Rebalance()
		if err != nil {
			d.dm.logger.ErrorWith("Failed to rebalance after AddJob", "deploy", d.Name, "job", rjob.Name, "err", err)
			return err
		}
	} else {
		d.dm.logger.WarnWith("Add job to function - Job already exist", "function", d.Name, "job", rjob.Name)
		return nil
	}

	return nil
}

// remove job while the deployment is working
func (d *Deployment) RemoveJob(job *Job, force bool) error {

	if job.IsStopping {
		d.dm.logger.WarnWith("RemoveJob - Job already stopping", "function", d.Name, "job", job.Name)
		return nil
	}

	job.Stop(d.procs)
	if job.assignedTasks == 0 {
		d.finalizeRemoveJob(job)
	}
	return nil
}

func (d *Deployment) finalizeRemoveJob(job *Job) error {
	d.dm.logger.InfoWith("finalize RemoveJob", "function", d.Name, "job", job.Name)
	delete(d.jobs, job.Name)
	return nil
}

// clear all deployment resources before a delete
func (d *Deployment) ClearDeployment() error {

	// delete links from proc to jobs and deployments
	for _, proc := range d.procs {
		proc.ClearAll()
	}

	// Delete Jobs from persistent stor
	d.dm.ctx.DeleteJobRecords(d.jobs)

	return nil
}
