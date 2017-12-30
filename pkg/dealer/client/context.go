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
	"github.com/valyala/fasthttp"
	"github.com/nuclio/nuclio-sdk"
	"fmt"
	"github.com/pkg/errors"
)

type Context struct {
	logger     nuclio.Logger
	httpClient *fasthttp.HostClient
	Url string
}

func NewContext(parentLogger nuclio.Logger, url string) (*Context, error) {
	newClient := &Context{
		logger: parentLogger.GetChild("client").(nuclio.Logger),
		httpClient: &fasthttp.HostClient{
			Addr: url,
		},
		Url: url,
	}

	return newClient, nil
}

func (c *Context) SetUrl(url string) {
	c.Url = url
	c.httpClient = &fasthttp.HostClient{Addr: url}
}

func (c *Context) SendRequest(method string,
	uri string,
	headers map[string]string,
	body []byte,
	releaseResponse bool) (*Response, error) {

	var success bool
	request := fasthttp.AcquireRequest()
	response := c.allocateResponse()

	// init request
	request.SetRequestURI(uri)
	request.Header.SetMethod(method)
	request.SetBody(body)

	if headers != nil {
		for headerName, headerValue := range headers {
			request.Header.Add(headerName, headerValue)
		}
	}
	request.Header.SetContentType("application/json")

	// execute the request
	err := c.doRequest(request, response.response)
	if err != nil {
		err = errors.Wrapf(err, "Failed to send request %s", uri)
		goto cleanup
	}

	// did we get a 2xx response?
	success = response.response.StatusCode() >= 200 && response.response.StatusCode() < 300

	// make sure we got expected status
	if !success {
		err = fmt.Errorf("Failed GET with status %d", response.response.StatusCode())
		goto cleanup
	}

	cleanup:

	// we're done with the request - the response must be released by the user
	// unless there's an error
	fasthttp.ReleaseRequest(request)

	if err != nil {
		response.Release()
		return nil, err
	}

	// if the user doesn't need the response, release it
	if releaseResponse {
		response.Release()
		return nil, nil
	}

	return response, nil
}

func (c *Context) allocateResponse() *Response {
	return &Response{
		response: fasthttp.AcquireResponse(),
	}
}

func (c *Context) doRequest(request *fasthttp.Request, response *fasthttp.Response) error {
	c.logger.DebugWith("Sending request",
		"method", string(request.Header.Method()),
		"uri", string(request.Header.RequestURI()),
		"body", string(request.Body()),
	)

	err := c.httpClient.Do(request, response)
	if err != nil {
		return err
	}

	// log the response
	c.logger.DebugWith("Got response",
		"statusCode", response.Header.StatusCode(),
		"body", string(response.Body()),
	)

	return nil
}

type Response struct {
	response *fasthttp.Response

	// hold a decoded output, if any
	Output interface{}
}

func (r *Response) Release() {
	fasthttp.ReleaseResponse(r.response)
}

func (r *Response) Body() []byte {
	return r.response.Body()
}

func EmulatedResp(body []byte) *Response {
	resp := fasthttp.Response{}
	resp.SetBody(body)
	return &Response{ response: &resp }
}