package testlib

import (
	"testing"
	"github.com/nuclio/nuclio-sdk"
	"github.com/nuclio/nuclio/pkg/zap"
	"github.com/pkg/errors"
	"github.com/nuclio/nuclio/pkg/dealer/jobs"
	"github.com/nuclio/nuclio/cmd/dealer/app"
)

//type testContext struct {
//	test     *testing.T
//	dealer   *Dealer
//	logger   nuclio.Logger
//	unique   int
//}
//
//func (tc *testContext) uniqueStr() string {
//	tc.unique++
//	return fmt.Sprintf("%04x", tc.unique)
//}

func TestDealer_Start(t *testing.T) {
	logger, _ := createLogger(false)

	dealer, err := app.NewDealer(logger, &jobs.ManagerContextConfig{
		DisablePush: true,
		StorePath:   "",
	})
	CheckErr(t, err)
	ctx := NewTestContext(t, dealer, logger)

	dealer.Start()

	fn := NewFunc(&ctx, "fn1", "")
	fn.SetJob("j1", &jobs.BaseJob{TotalTasks:5})
	fn.SetJob("j2", &jobs.BaseJob{TotalTasks:7})

	fn.UpdateDeployment(3)
	fn.ProcSum()

	fn.SetJob("j1", &jobs.BaseJob{TotalTasks:5, Disable:true})
	fn.UpdateDeployment(3)
	fn.ProcSum()
	fn.UpdateDeployment(1)

	fn.ProcSum()
	fn.UpdateDeployment(2)
	fn.ProcSum()

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