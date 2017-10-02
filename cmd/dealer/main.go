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

package main

import (
	"flag"
	"fmt"
	"os"
	"github.com/nuclio/nuclio/cmd/dealer/app"
	"github.com/nuclio/nuclio/pkg/dealer/portal"
	"github.com/nuclio/nuclio/pkg/dealer/jobs"
	"github.com/nuclio/nuclio-sdk"
	"github.com/nuclio/nuclio/pkg/zap"
	"github.com/pkg/errors"
)

func run() error {
	configPath := flag.String("config", "", "Path of configuration file")
	verbose := flag.Bool("v", false, "Verbose")
	flag.Parse()

	logger, _ := createLogger(*verbose)

	dealer, err := app.NewJobManager(*configPath, logger)
	if err != nil {
		return err
	}

	err = dealer.Start()
	if err != nil {
		return err
	}

	//Tests(dealer)

	listenPort := 3000
	portal, err := portal.NewPortal(logger, &dealer.Ctx, listenPort)
	if err != nil {
		return err
	}

	return portal.Start()
}

func main() {

	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to run dealer: %s", err)

		os.Exit(1)
	}
}

func createLogger(verbose bool) (nuclio.Logger, error) {
	var loggerLevel nucliozap.Level

	if verbose {
		loggerLevel = nucliozap.DebugLevel
	} else {
		loggerLevel = nucliozap.InfoLevel
	}

	logger, err := nucliozap.NewNuclioZap("nuclio-dealer", loggerLevel)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create logger")
	}

	return logger, nil

}


func Tests(jm *app.JobManager) {
	jm.AddJob(&jobs.Job{Name:"myjob",FunctionURI:"f1",ExpectedProc:3, TotalTasks:11})
	//jm.AddProcess(&jobs.Process{Name:"p1",Function:"f1",Version:"latest", IP:"192.168.1.133", Port:5000})
	//fmt.Println(jm.Jobs["myjob.default"].AsString())
	//jm.AddProcess(&jobs.Process{Name:"p2",Function:"f1",Version:"latest"})
	//fmt.Println(jm.Jobs["myjob.default"].AsString())
	//jm.Jobs["myjob.default"].UpdateNumProcesses(2,true)
	//fmt.Println(jm.Jobs["myjob.default"].AsString())

}


