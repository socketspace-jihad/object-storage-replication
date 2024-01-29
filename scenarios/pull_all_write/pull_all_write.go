package pullallwrite

import (
	"log"
	"sync"

	"github.com/socketspace-jihad/s3-sync-replication/destination"
	"github.com/socketspace-jihad/s3-sync-replication/scenarios"
	"github.com/socketspace-jihad/s3-sync-replication/serializer"
	"github.com/socketspace-jihad/s3-sync-replication/source"
)

type PullAllWrite struct {
	source.Source
	destination.Destination
}

func NewPullAllWrite(src source.Source, dest destination.Destination) scenarios.Scenarios {
	return &PullAllWrite{
		Source:      src,
		Destination: dest,
	}
}

func (p *PullAllWrite) Validate() error {
	return nil
}

func (p *PullAllWrite) Run() error {
	files := p.Source.PullAll()
	log.Println("FOUND FILES:", len(files))
	wg := &sync.WaitGroup{}
	for _, file := range files {
		wg.Add(1)
		go func(file chan serializer.SEF) {
			defer wg.Done()
			p.Destination.Write(file)
		}(file)
	}
	wg.Wait()
	return nil
}

func init() {
	scenarios.RegisterScenarios("pull_all_write", NewPullAllWrite)
}
