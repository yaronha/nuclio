package main

import (
	"text/template"
	"os"
)

var funcMap = template.FuncMap{}


type Location struct {
	Path        string
	Namespace   string
	Rule        string
	Service     string
	Upstream    string
}

type Server struct {
	Host    string
	Port    int
	Locations []*Location
}

type Configuration struct {
	Servers []*Server
	Backends []*Backend
}

type Backend struct {
	// Name represents an unique apiv1.Service name formatted as <namespace>-<name>-<port>
	Name    string             `json:"name"`
	Service string             `json:"service,omitempty"`
	Port    int `json:"port"`
	Endpoints []Endpoint `json:"endpoints,omitempty"`
}

type Endpoint struct {
	// Address IP address of the endpoint
	Address string `json:"address"`
	// Port number of the TCP port
	Port string `json:"port"`
}


func NewTemplate() (*template.Template, error) {
	tmpl, err := template.New("nginx.tmpl").Funcs(funcMap).Parse(tmpltext)
	if err != nil {
		return nil, err
	}

	return tmpl, nil
}

func main() {

	bce := Backend{Name:"nam", Service:"svc", Port: 80}
	cfg := Configuration{Backends: []*Backend{&bce}}
	tmpl, err := NewTemplate()
	if err != nil {
		panic(err)
	}

	err = tmpl.Execute(os.Stdout, cfg)
	if err != nil {
		panic(err)
	}


}


const tmpltext = `
{{ $servers := .Servers }}
{{ $backends := .Backends }}
http {

    {{ range $name, $upstream := $backends }}

    upstream {{ $upstream.Name }} {
        least_conn;
        keepalive 32;

        {{ range $server := $upstream.Endpoints }}server {{ $server.Address }}:{{ $server.Port }} max_fails=0 fail_timeout=0;
        {{ end }}
    }

    {{ end }}


    {{ range $index, $server := $servers }}

    ## start server {{ $server.Hostname }}
    server {
        # server_name {{ $server.Hostname }};
        listen {{ $server.Port }};

        {{ range $location := $server.Locations }}

         location {{ $location.Path }} {
            set $namespace      "{{ $location.Namespace }}";
            set $ingress_name   "{{ $location.Rule }}";
            set $service_name   "{{ $location.Service }}";

             proxy_pass {{ $location.Upstream }}
        }

        {{ end }}

    }
    ## end server {{ $server.Hostname }}

    {{ end }}

}


`