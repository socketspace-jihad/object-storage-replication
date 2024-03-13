package main

import (
	"errors"
	"log"
	"os"

	"github.com/socketspace-jihad/s3-sync-replication/destination"
	_ "github.com/socketspace-jihad/s3-sync-replication/destination/s3"
	"github.com/socketspace-jihad/s3-sync-replication/scenarios"
	_ "github.com/socketspace-jihad/s3-sync-replication/scenarios/pull_all_write"
	_ "github.com/socketspace-jihad/s3-sync-replication/scenarios/pull_with_date_write"
	_ "github.com/socketspace-jihad/s3-sync-replication/scenarios/pull_with_hourly_prefix_write"
	_ "github.com/socketspace-jihad/s3-sync-replication/scenarios/pull_with_prefix_write"
	"github.com/socketspace-jihad/s3-sync-replication/source"
	_ "github.com/socketspace-jihad/s3-sync-replication/source/s3"
)

func envValidation() error {
	if _, ok := os.LookupEnv("SOURCE_OBJECT_STORAGE_PLATFORM"); !ok {
		return errors.New("environment not found: SOURCE_OBJECT_STORAGE_PLATFORM")
	}

	if _, ok := os.LookupEnv("DEST_OBJECT_STORAGE_PLATFORM"); !ok {
		return errors.New("environment not found: DEST_OBJECT_STORAGE_PLATFORM")
	}
	if _, ok := os.LookupEnv("REPLICATION_SCENARIOS"); !ok {
		return errors.New("environment not found: REPLICATION_SCENARIOS")
	}
	return nil
}

func init() {
	if err := envValidation(); err != nil {
		panic(err)
	}
}

func main() {

	src, err := source.GetSource(os.Getenv("SOURCE_OBJECT_STORAGE_PLATFORM"))
	if err != nil {
		panic(err)
	}
	if err := src().Validate(); err != nil {
		panic(err)
	}

	dest, err := destination.GetDestination(os.Getenv("DEST_OBJECT_STORAGE_PLATFORM"))
	if err != nil {
		panic(err)
	}
	if err := dest().Validate(); err != nil {
		panic(err)
	}

	scn, err := scenarios.GetScenarios(os.Getenv("REPLICATION_SCENARIOS"))
	if err != nil {
		panic(err)
	}

	if err := scn(src(), dest()).Validate(); err != nil {
		panic(err)
	}
	if err := scn(src(), dest()).Run(); err != nil {
		panic(err)
	}

	log.Println("REPLICATION COMPLETED!")
	log.Println("SYNCED")
}
