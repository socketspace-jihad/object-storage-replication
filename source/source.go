package source

import (
	"time"

	"github.com/socketspace-jihad/s3-sync-replication/serializer"
)

type Source interface {
	PullAll() []chan serializer.SEF
	PullWithDateFilter(time.Time) []chan serializer.SEF
	PullWithNameFilter(string) []chan serializer.SEF
	Validate() error
}
