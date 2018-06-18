package testlib

import (
	"testing"
	"github.com/pkg/errors"
	"github.com/nuclio/nuclio/pkg/dealer/jobs"
	"github.com/nuclio/nuclio/cmd/dealer/app"
	"github.com/nuclio/zap"
	"github.com/nuclio/logger"
	"fmt"
)


func TestDealer_Start(t *testing.T) {
	logger, _ := createLogger(true)

	dealer, err := app.NewDealer(logger, &jobs.ManagerContextConfig{
		DisablePush: true,
		StorePath:   "",
		SendStopMessage: true,
	})
	CheckErr(t, err)
	ctx := NewTestContext(t, dealer, logger)

	dealer.Start()

	fn := NewFunc(&ctx, "fn1", "")
	fn.SetJob("j1", &jobs.BaseJob{TotalTasks:5})
	fn.SetJob("j2", &jobs.BaseJob{TotalTasks:7})

	fn.AddProc("P1","34.230.69.143:8801")
	fn.AddProc("P2","34.230.69.143:8802")
	fn.AddProc("P3","34.230.69.143:8803")

	fn.UpdateDeployment(3)
	info := fn.ProcInfo()

	if info.JobTasks["j1"].active != 5 || info.JobTasks["j2"].active != 7 {
		t.Fatal("active job tasks dont match requiered", info.JobTasks)
	}

	if len(info.ProcsTasks) != 3 {
		t.Fatal("number of processes dont match desired scale", info.ProcsTasks)
	}

	if !info.ProcTasksInRange(4,4,true) {
		t.Fatal("proc tasks not in range", info.ProcsTasks)
	}
	fmt.Println("INFO:", info)

	fn.SetJob("j1", &jobs.BaseJob{TotalTasks:5, Disable:true})
	fn.UpdateDeployment(3)
	info = fn.ProcInfo()
	fmt.Println("INFO-disable:", info)
	waitMs(20)
	info = fn.ProcInfo()
	fmt.Println("INFO-disable2:", info)
	fn.UpdateDeployment(1)

	info = fn.ProcInfo()
	fmt.Println("INFO-scaledown:", info)
	fn.UpdateDeployment(2)
	fn.ProcInfo()

}


func createLogger(verbose bool) (logger.Logger, error) {
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