package main

import (
	"github.com/nuclio/nuclio-sdk"
	"github.com/nuclio/nuclio/pkg/zap"
	"flag"
	"github.com/pkg/errors"
	"github.com/nuclio/nuclio/pkg/starter"
)

func main() {

	err := run()
	if err != nil {
		panic(err)
	}

}

func run() error {

	verbose := flag.Bool("d", true, "Verbose")
	//kubeconf := flag.String("k", "config", "Path to a kube config. Only required if out-of-cluster.")
	kubeconf := flag.String("k", "config", "Path to a kube config. Only required if out-of-cluster.")
	namespace := flag.String("n", "default", "Namespace")
	flag.Parse()

	logger, _ := createLogger(*verbose)

	starter, err := starter.NewStarter(logger, starter.StarterConfig{
		Verbose: *verbose, Namespace: *namespace, Kubeconf: *kubeconf,
	})

	if err != nil {
		return err
	}

	starter.Start()

	return nil

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
