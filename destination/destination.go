package destination

import (
	"sync"

	"github.com/socketspace-jihad/s3-sync-replication/serializer"
)

type Destination interface {
	WriteOverride(chan serializer.SEF) error
	Write(<-chan serializer.SEF, *sync.WaitGroup) error
	Validate() error
}
