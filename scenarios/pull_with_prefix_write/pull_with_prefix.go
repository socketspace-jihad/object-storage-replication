package pullallwrite

import (
	"errors"
	"log"
	"os"
	"sync"

	"github.com/socketspace-jihad/s3-sync-replication/destination"
	"github.com/socketspace-jihad/s3-sync-replication/scenarios"
	"github.com/socketspace-jihad/s3-sync-replication/serializer"
	"github.com/socketspace-jihad/s3-sync-replication/source"
)

type PullWithPrefix struct {
	source.Source
	destination.Destination
}

func NewPullPullWithPrefixWrite(src source.Source, dest destination.Destination) scenarios.Scenarios {
	return &PullWithPrefix{
		Source:      src,
		Destination: dest,
	}
}

func (p *PullWithPrefix) Validate() error {
	if _, ok := os.LookupEnv("PREFIX"); !ok {
		return errors.New("environment not found: PREFIX")
	}
	return nil
}

func (p *PullWithPrefix) Run() error {
	commChan := make(chan serializer.SEF)
	wg := &sync.WaitGroup{}
	log.Println("PULLING SOURCE..")
	go p.Destination.Write(commChan, wg)
	p.Source.PullWithPrefix(commChan, wg, os.Getenv("PREFIX"))
	log.Println("WAITING THE REPLICATION TO BE COMPLETED..")
	wg.Wait()
	close(commChan)
	return nil
}

func init() {
	scenarios.RegisterScenarios("pull_with_prefix_write", NewPullPullWithPrefixWrite)
}
