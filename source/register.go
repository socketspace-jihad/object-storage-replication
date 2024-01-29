package source

import (
	"errors"
)

type SourceFactory func() Source

var SourceMap map[string]SourceFactory = make(map[string]SourceFactory)

func RegisterSource(name string, factory SourceFactory) {
	SourceMap[name] = factory
}

func GetSource(name string) (SourceFactory, error) {
	if _, ok := SourceMap[name]; !ok {
		return nil, errors.New("source factory not registered")
	}
	return SourceMap[name], nil
}
