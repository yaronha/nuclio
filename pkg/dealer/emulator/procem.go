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
	"github.com/nuclio/nuclio-sdk"
	"github.com/nuclio/nuclio/pkg/zap"
	"github.com/pkg/errors"
	"flag"
	"net"
	"fmt"
	"os"
	"github.com/nuclio/nuclio/pkg/dealer/client"
	"os/signal"
	"syscall"
	"github.com/nuclio/nuclio/pkg/dealer/emulator/processor"
	"encoding/json"
	"github.com/nuclio/nuclio/pkg/dealer/jobs"
)

func main() {

	name := flag.String("n", "test", "Process name")
	ns := flag.String("s", "default", "Process namespace")
	function := flag.String("f", "f1", "Function name")
	version := flag.String("r", "latest", "Function version")
	alias := flag.String("a", "", "Function alias")
	url := flag.String("u", "", "Dealer ip:port")
	port := flag.Int("p",8077,"local port")
	reportedIP := flag.String("i","","reported IP")
	verbose := flag.Bool("v", false, "Verbose")
	flag.Parse()

	ip := *reportedIP
	if ip == "" {
		var err error
		ip, err = getMyIP()
		if err != nil {
			fmt.Printf("Failed to get IP: %s", err)
			os.Exit(1)
		}
	}

	logger, _ := createLogger(*verbose)
	proc := jobs.Process{ BaseProcess: jobs.BaseProcess{Name:*name, Namespace:*ns, Function:*function, Version:*version, Alias:*alias, IP:ip, Port:*port}}

	client, _ := client.NewContext(logger, *url)
	headers := map[string]string{}
	body, err := json.Marshal(proc)
	if err != nil {
		fmt.Printf("Failed to Marshal: %s", err)
		os.Exit(1)
	}


	if *url != "" {
		path := fmt.Sprintf("http://%s/procs", *url)
		_, err = client.SendRequest("POST", path, headers, body, false)
		if err != nil {
			fmt.Printf("Failed to send: %s", err)
			os.Exit(1)
		}
	}

	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		fmt.Printf("Deleting Process %s\n", *name)
		if *url != "" {
			path := fmt.Sprintf("http://%s/procs/%s/%s", *url, *ns, *name)
			client.SendRequest("DELETE", path, headers, body, false)
		}
		os.Exit(1)
	}()


	newEmulator, _ := processor.NewProcessEmulator(logger, &proc)
	err = newEmulator.Start()
	if err != nil {
		fmt.Printf("Failed to start emulator: %s", err)
		os.Exit(1)
	}

}

func getMyIP() (string,error) {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return "", err
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)
	return fmt.Sprintf("%s",localAddr.IP), nil
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


