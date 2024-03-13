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
	commChan := make(chan serializer.SEF, 2048)
	wg := &sync.WaitGroup{}
	log.Println("PULLING SOURCE..")
	go p.Destination.Write(commChan, wg)
	p.Source.PullAll(commChan, wg)
	log.Println("WAITING THE REPLICATION TO BE COMPLETED..")
	wg.Wait()
	close(commChan)
	return nil
}

func init() {
	scenarios.RegisterScenarios("pull_all_write", NewPullAllWrite)
}
