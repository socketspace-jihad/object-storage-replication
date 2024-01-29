package destination

import (
	"errors"
)

type DestinationFactory func() Destination

var DestinationMap map[string]DestinationFactory = make(map[string]DestinationFactory)

func RegisterDestination(name string, factory DestinationFactory) {
	DestinationMap[name] = factory
}

func GetDestination(name string) (DestinationFactory, error) {
	if _, ok := DestinationMap[name]; !ok {
		return nil, errors.New("destination factory not registered")
	}
	return DestinationMap[name], nil
}
