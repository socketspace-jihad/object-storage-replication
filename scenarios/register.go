package scenarios

import (
	"errors"

	"github.com/socketspace-jihad/s3-sync-replication/destination"
	"github.com/socketspace-jihad/s3-sync-replication/source"
)

type ScenariosFactory func(source.Source, destination.Destination) Scenarios

var ScenariosMap map[string]ScenariosFactory = make(map[string]ScenariosFactory)

func RegisterScenarios(name string, factory ScenariosFactory) {
	ScenariosMap[name] = factory
}

func GetScenarios(name string) (ScenariosFactory, error) {
	if _, ok := ScenariosMap[name]; !ok {
		return nil, errors.New("scenarios didn't registered")
	}
	return ScenariosMap[name], nil
}
