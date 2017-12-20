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

package client

import (
	"github.com/nuclio/nuclio-sdk"
)

func NewAsyncClient(logger nuclio.Logger) (*AsyncClient, error) {

	newAsyncClient := AsyncClient{logger: logger}
	inChan := make(chan *ChanRequest, 100)
	newAsyncClient.InChannel = inChan
	return &newAsyncClient, nil
}

type ChanRequest struct {
	Method     string
	HostURL    string
	Url        string
	Headers    map[string]string
	Body       []byte
	NeedResp   bool
	ReturnChan chan *Response
}

type AsyncClient struct {
	logger    nuclio.Logger
	InChannel chan *ChanRequest
}

func (a *AsyncClient) Start(workers int) error {

	for wk := 1; wk <= workers; wk++ {
		go func() {
			for {
				request, ok := <-a.InChannel

				if !ok {
					break
				}

				client, _ := NewContext(a.logger, request.HostURL)
				resp, err := client.SendRequest(request.Method, request.Url, request.Headers, request.Body, request.NeedResp)
				if err != nil {
					a.logger.Error("Failed to send: %s", err)
				} else {
					request.ReturnChan <- resp
				}

			}
		}()
	}

	return nil
}

func (a *AsyncClient) Submit(request *ChanRequest) {
	a.InChannel <- request
}
