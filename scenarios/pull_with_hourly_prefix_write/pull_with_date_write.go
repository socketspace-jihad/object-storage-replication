package pullwithdatewrite

import (
	"errors"
	"fmt"
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

type PullWithHourlyPrefixWrite struct {
	source.Source
	destination.Destination
}

func (p *PullWithHourlyPrefixWrite) Validate() error {
	if _, ok := os.LookupEnv("START_DATE"); !ok {
		return errors.New("environment not found: START_DATE")
	}
	if _, ok := os.LookupEnv("PREFIX"); !ok {
		return errors.New("environment not found: PREFIX")
	}
	if _, ok := os.LookupEnv("TARGET_SCOPE"); !ok {
		return errors.New("environment not found: TARGET_SCOPE")
	}
	return nil
}

func NewPullWithHourlyPrefixWrite(src source.Source, dest destination.Destination) scenarios.Scenarios {
	return &PullWithHourlyPrefixWrite{
		Source:      src,
		Destination: dest,
	}
}

func getPrefixTime(t time.Time, s string, prefix string) string {
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
	return fmt.Sprintf("%v%v-%v-%v-%v", prefix, t.Year(), fmt.Sprintf("%02d", int(t.Month())), fmt.Sprintf("%02d", t.Day()), fmt.Sprintf("%02d", t.Hour()))
}

func (p *PullWithHourlyPrefixWrite) Run() error {
	t := getPrefixTime(time.Now().UTC(), os.Getenv("START_DATE"), os.Getenv("PREFIX"))
	commChan := make(chan serializer.SEF)
	wg := &sync.WaitGroup{}
	go p.Destination.Write(commChan, wg)
	p.Source.PullWithPrefix(commChan, wg, t)
	wg.Wait()
	close(commChan)
	return nil
}

func init() {
	scenarios.RegisterScenarios("pull_with_hourly_prefix_write", NewPullWithHourlyPrefixWrite)
}
