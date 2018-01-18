package core

import (
	"github.com/hashicorp/go-immutable-radix"
	"fmt"
	"github.com/nuclio/nuclio-sdk"
)

func NewPathRadix(logger nuclio.Logger) *PathRadix {
	return &PathRadix{ trees: map[string]*iradix.Tree{}, logger: logger.GetChild("PathRadix")}
}

type PathRadix struct {
	trees    map[string]*iradix.Tree
	logger   nuclio.Logger
}

func (pr *PathRadix) UpdatePaths(function *FunctionRecord) {

	for _, ingress := range function.Ingresses {
		radix, ok := pr.trees[ingress.Host]

		if !ok {
			radix = iradix.New()
		}

		for _, path := range ingress.Paths {
			radix, _, _ = radix.Insert([]byte(path), function)
		}
		pr.trees[ingress.Host] = radix
	}
}

func (pr *PathRadix) DeletePaths(function *FunctionRecord) error {

	for _, ingress := range function.Ingresses {
		radix, ok := pr.trees[ingress.Host]

		if !ok {
			return fmt.Errorf("Delete paths error, host %s does not exist", ingress.Host)
		}

		for _, path := range ingress.Paths {
			_, _, set := radix.Delete([]byte(path))
			if !set {
				pr.logger.ErrorWith("path was not set in DeletePaths", "host", ingress.Host, "path", path)
			}
		}
	}

	return nil
}

func (pr *PathRadix) Lookup(host, path string) (*FunctionRecord, bool) {

	radix, ok := pr.trees[host]

	if ok {
		_, value, found := radix.Root().LongestPrefix([]byte(path))

		if found {
			return value.(*FunctionRecord), found
		}
	}

	radix, ok = pr.trees[""]
	if ok {
		_, value, found := radix.Root().LongestPrefix([]byte(path))

		if found {
			return value.(*FunctionRecord), found
		}
	}


	return nil, false

}

func (pr *PathRadix) WalkTree(host string) map[string]*FunctionRecord {
	funcMap := map[string]*FunctionRecord{}

	radix, ok := pr.trees[host]
	if !ok {
		return funcMap
	}

	iter := radix.Root().Iterator()
	for {
		key, value, more := iter.Next()
		if !more {
			break
		}
		funcMap[string(key)] = value.(*FunctionRecord)
	}

	return funcMap

}