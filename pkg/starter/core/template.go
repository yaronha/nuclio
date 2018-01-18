package core

import (
	"os"
	"text/template"
)

var funcMap = template.FuncMap{}

type Configuration struct {
	Servers []*Server
	Backends []*Backend
}

type Server struct {
	Host       string
	Port       int
	Locations  map[string]*FunctionRecord
}

type Backend struct {
	// Name represents an unique apiv1.Service name formatted as <namespace>-<name>-<port>
	Namespace     string
	Name      string             `json:"name"`
	Function  string             `json:"service,omitempty"`
	Port      int                `json:"port"`
	Endpoints []string         `json:"endpoints,omitempty"`
}


func NewTemplate() (*template.Template, error) {
	tmpl, err := template.New("nginx.tmpl").Funcs(funcMap).Parse(tmpltext)
	if err != nil {
		return nil, err
	}

	return tmpl, nil
}

func CreateTemplate(funcDict *FuncDirectory) error {

	servers := []*Server{}
	backends := []*Backend{
		&Backend{Namespace:"", Name:"default_backend", Port: 8080, Endpoints: []string{"1.1.1.1"}},
	}
	for host, _ := range funcDict.Radix.trees {
		server := Server{Host:host, Port:8088, Locations: funcDict.Radix.WalkTree(host)}
		servers = append(servers, &server)
	}
	for _, fn := range funcDict.GetFunctions() {
		if len(fn.EndPoints) > 0 {
			backend := Backend{Namespace:fn.Namespace, Name:fn.CRName, Function: fn.Function, Port: fn.ApiPort, Endpoints: fn.EndPoints}
			backends = append(backends, &backend)
		}
	}
	cfg := Configuration{Backends: backends, Servers: servers}
	tmpl, err := NewTemplate()
	if err != nil {
		return err
	}

	err = tmpl.Execute(os.Stdout, cfg)
	if err != nil {
		return err
	}

	return nil
}


const tmpltext = `
{{ $servers := .Servers }}
{{ $backends := .Backends }}
http {

    {{ range $index, $upstream := $backends }}
    upstream {{ $upstream.Name }} {
        least_conn;
        keepalive 32;
        {{ range $server := $upstream.Endpoints }}server {{ $server }}:{{ $upstream.Port }} max_fails=0 fail_timeout=0;
        {{ end }}
    }
    {{ end }}
    {{ range $index, $server := $servers }}

    ## start server {{ $server.Host }}
    server {
        # server_name {{ $server.Host }};
        listen {{ $server.Port }};
        {{ range $path, $location := $server.Locations }}
        location {{ $path }} {
            set $namespace      "{{ $location.Namespace }}";
            set $ingress_name   "{{ $location.Function }}";
            set $service_name   "{{ $location.CRName }}";

            proxy_pass {{ $location.CRName }}
        }
        {{ end }}

    }
    ## end server {{ $server.Host }}
    {{ end }}

}


`
