package core

import (
	"github.com/hashicorp/go-immutable-radix"
)

func NewPathRadix() *PathRadix {
	return &PathRadix{ trees: map[string]*iradix.Tree{}}
}

type PathRadix struct {
	trees  map[string]*iradix.Tree
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