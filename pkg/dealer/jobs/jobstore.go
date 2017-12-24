package jobs

import (
	"encoding/json"
	"github.com/nuclio/nuclio-sdk"
	"io/ioutil"
	"os"
	"path"
)

type JobStore interface {
	//GetJob(namespace, function, name string) (*jobs.Job, error)
	DelJob(job *Job) error
	SaveJob(job *Job) error
	ListJobs(namespace string) ([]*JobMessage, error)
}

func NewJobFileStore(path string, logger nuclio.Logger) *JobFileStore {
	return &JobFileStore{Path: path, logger: logger}
}

type JobFileStore struct {
	Path   string
	logger nuclio.Logger
}

func (fs *JobFileStore) ListJobs(namespace string) ([]*JobMessage, error) {
	// TODO: support namespaces

	jobList := []*JobMessage{}
	if fs.Path == "" {
		return jobList, nil
	}

	files, err := ioutil.ReadDir(fs.Path)
	if err != nil {
		fs.logger.ErrorWith("cant list Job file dir", "path", fs.Path, "err", err)
		return jobList, err
	}

	for _, f := range files {
		fullPath := path.Join(fs.Path, f.Name())
		fileBytes, err := ioutil.ReadFile(fullPath)
		if err != nil {
			fs.logger.ErrorWith("cant list read file dir", "path", fullPath, "err", err)
			continue
		}

		job := JobMessage{}
		err = json.Unmarshal(fileBytes, &job)
		if err != nil {
			fs.logger.ErrorWith("cant unmarshal job file", "path", fullPath, "err", err)
			continue
		}
		jobList = append(jobList, &job)
	}
	return jobList, nil
}

func (fs *JobFileStore) DelJob(job *Job) error {
	filename := job.Namespace + "_" + job.Function + "_" + job.Name + ".json"
	fullpath := path.Join(fs.Path, filename)
	return os.Remove(fullpath)
}

func (fs *JobFileStore) SaveJob(job *Job) error {

	if fs.Path == "" {
		return nil
	}

	filename := job.Namespace + "_" + job.Function + "_" + job.Name + ".json"
	file, err := os.OpenFile(
		path.Join(fs.Path, filename),
		os.O_WRONLY|os.O_TRUNC|os.O_CREATE,
		0666,
	)
	if err != nil {
		fs.logger.ErrorWith("cant open file", "file", filename, "err", err)
		return err
	}
	defer file.Close()

	jobMessage := JobMessage{Job: *job}
	jobMessage.Tasks = []TaskMessage{}

	for _, task := range job.tasks {
		// Store tasks only if they have data to persist (Completion, Checkpoint, Progress)
		if task.state == TaskStateCompleted || task.CheckPoint != nil || task.Progress != 0 {
			taskRecord := BaseTask{Id: task.Id, CheckPoint: task.CheckPoint, Progress: task.Progress}
			jobMessage.Tasks = append(jobMessage.Tasks, TaskMessage{BaseTask: taskRecord, State: task.state})
		}
	}

	// Write bytes to file
	byteSlice, err := json.Marshal(jobMessage)
	if err != nil {
		fs.logger.ErrorWith("cant Marshal file", "file", filename, "err", err)
		return err
	}
	_, err = file.Write(byteSlice)
	if err != nil {
		fs.logger.ErrorWith("cant write file", "file", filename, "err", err)
		return err
	}

	return nil
}
