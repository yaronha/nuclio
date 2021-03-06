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

package shell

import (
	"github.com/nuclio/nuclio/pkg/processor/build/runtime"
)

type shell struct {
	*runtime.AbstractRuntime
}

// GetName returns the name of the runtime, including version if applicable
func (s *shell) GetName() string {
	return "shell"
}

// GetProcessorDockerfilePath returns the contents of the appropriate Dockerfile, with which we'll build
// the processor image
func (s *shell) GetProcessorDockerfileContents() string {
	return `ARG NUCLIO_LABEL=latest
ARG NUCLIO_ARCH=amd64
ARG NUCLIO_BASE_IMAGE=alpine:3.6
ARG NUCLIO_ONBUILD_IMAGE=nuclio/processor:${NUCLIO_LABEL}-${NUCLIO_ARCH}

# Supplies processor uhttpc, used for healthcheck
FROM nuclio/uhttpc:0.0.1-amd64 as uhttpc

# Supplies processor binary, wrapper
FROM ${NUCLIO_ONBUILD_IMAGE} as processor

# From the base image
FROM ${NUCLIO_BASE_IMAGE}

# Copy required objects from the suppliers
COPY --from=processor /home/nuclio/bin/processor /usr/local/bin/processor
COPY --from=uhttpc /home/nuclio/bin/uhttpc /usr/local/bin/uhttpc

# Copy the handler directory to /opt/nuclio
COPY handler /opt/nuclio

# Readiness probe
HEALTHCHECK --interval=1s --timeout=3s CMD /usr/local/bin/uhttpc --url http://localhost:8082/ready || exit 1

# Set node modules path
ENV NODE_PATH=/usr/local/lib/node_modules

# Run processor with configuration and platform configuration
CMD [ "processor", "--config", "/etc/nuclio/config/processor/processor.yaml", "--platform-config", "/etc/nuclio/config/platform/platform.yaml" ]
`
}

// GetProcessorImageObjectPaths returns the paths of all objects that should reside in the handler
// directory
func (s *shell) GetHandlerDirObjectPaths() []string {
	if s.FunctionConfig.Spec.Build.Path != "/dev/null" {
		return s.AbstractRuntime.GetHandlerDirObjectPaths()
	}

	return []string{}
}
