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
	"encoding/json"
	"flag"
	"fmt"
	"github.com/nuclio/nuclio-sdk"
	"github.com/nuclio/nuclio/pkg/dealer/client"
	"github.com/nuclio/nuclio/pkg/dealer/emulator/processor"
	"github.com/nuclio/nuclio/pkg/dealer/jobs"
	"github.com/nuclio/nuclio/pkg/zap"
	"github.com/pkg/errors"
	"net"
	"os"
	"os/signal"
	"syscall"
)

func main() {

	host, _ := os.Hostname()
	name := flag.String("n", host, "Process name")
	ns := flag.String("s", "default", "Process namespace")
	function := flag.String("f", os.Getenv("NUCLIO_FUNCTION_NAME"), "Function name")
	version := flag.String("r", "latest", "Function version")
	alias := flag.String("a", "latest", "Function alias")
	url := flag.String("u", os.Getenv("DEALER_URL"), "Dealer ip:port")
	port := flag.Int("p", 8077, "local port")
	reportedIP := flag.String("i", os.Getenv("REPORTED_IP"), "reported IP")
	verbose := flag.Bool("v", true, "Verbose")
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
	proc := jobs.ProcessMessage{BaseProcess: jobs.BaseProcess{Name: *name, Namespace: *ns, Function: *function, Version: *version, Alias: *alias, IP: ip, Port: *port}}

	client, _ := client.NewContext(logger, *url)
	headers := map[string]string{}
	body, err := json.Marshal(proc)
	if err != nil {
		fmt.Printf("Failed to Marshal: %s", err)
		os.Exit(1)
	}

	newEmulator, _ := processor.NewProcessEmulator(logger, &proc)
	if *url != "" {
		path := fmt.Sprintf("http://%s/procs", *url)
		resp, err := client.SendRequest("POST", path, headers, body, false)
		if err != nil {
			fmt.Printf("Failed to send: %s\n", err)
		}

		procMsg := &jobs.ProcessMessage{}
		err = json.Unmarshal(resp.Body(), procMsg)
		if err != nil {
			logger.ErrorWith("Failed to Unmarshal process resp", "body", string(resp.Body()), "err", err)
		}
		newEmulator.Proc.ProcessUpdates(procMsg)
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

	err = newEmulator.Start()
	if err != nil {
		fmt.Printf("Failed to start emulator: %s", err)
		os.Exit(1)
	}

}

func getMyIP() (string, error) {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return "", err
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)
	return fmt.Sprintf("%s", localAddr.IP), nil
}

func createLogger(verbose bool) (nuclio.Logger, error) {
	var loggerLevel nucliozap.Level

	if verbose {
		loggerLevel = nucliozap.DebugLevel
	} else {
		loggerLevel = nucliozap.InfoLevel
	}

	logger, err := nucliozap.NewNuclioZapCmd("dealer-pemu", loggerLevel)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create logger")
	}

	return logger, nil

}
