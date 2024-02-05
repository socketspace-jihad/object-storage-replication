package pullwithdatewrite

import (
	"errors"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/socketspace-jihad/s3-sync-replication/destination"
	"github.com/socketspace-jihad/s3-sync-replication/scenarios"
	"github.com/socketspace-jihad/s3-sync-replication/serializer"
	"github.com/socketspace-jihad/s3-sync-replication/source"
)

type PullWithDateWrite struct {
	source.Source
	destination.Destination
}

func (p *PullWithDateWrite) Validate() error {
	if _, ok := os.LookupEnv("START_DATE"); !ok {
		return errors.New("environment not found: START_DATE")
	}
	return nil
}

func NewPullWithDateWrite(src source.Source, dest destination.Destination) scenarios.Scenarios {
	return &PullWithDateWrite{
		Source:      src,
		Destination: dest,
	}
}

func getNewTime(t time.Time, s string) time.Time {
	length := len(s)
	metric := s[length-1]
	num, err := strconv.Atoi(s[1 : length-1])
	if err != nil {
		panic(err)
	}
	switch string(metric) {
	case "d":
		log.Printf("GETTING DATA FROM %v DAY AGO..\n", num)
		t = t.Add(time.Duration(-num) * 24 * time.Hour)
	case "h":
		log.Printf("GETTING DATA FROM %v HOUR AGO..\n", num)
		t = t.Add(time.Duration(-num) * time.Hour)
	case "m":
		log.Printf("GETTING DATA FROM %v MINUTE AGO..\n", num)
		t = t.Add(time.Duration(-num) * time.Minute)
	case "s":
		log.Printf("GETTING DATA FROM %v SECOND AGO..\n", num)
		t = t.Add(time.Duration(-num) * time.Second)
	}
	return t
}

func (p *PullWithDateWrite) Run() error {
	t := getNewTime(time.Now(), os.Getenv("START_DATE"))
	commChan := make(chan serializer.SEF)
	wg := &sync.WaitGroup{}
	p.Source.PullWithDateFilter(commChan, wg, t)
	go p.Destination.Write(commChan, wg)
	wg.Wait()
	close(commChan)
	return nil
}

func init() {
	scenarios.RegisterScenarios("pull_with_date_write", NewPullWithDateWrite)
}
