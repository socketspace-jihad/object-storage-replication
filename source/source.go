package source

import (
	"sync"
	"time"

	"github.com/socketspace-jihad/s3-sync-replication/serializer"
)

type Source interface {
	PullAll(chan serializer.SEF, *sync.WaitGroup) []chan serializer.SEF
	PullWithPrefix(chan serializer.SEF, *sync.WaitGroup, string) []chan serializer.SEF
	PullWithDateFilter(chan serializer.SEF, *sync.WaitGroup, time.Time) []chan serializer.SEF
	PullWithNameFilter(string) []chan serializer.SEF
	Validate() error
}
