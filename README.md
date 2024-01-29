## Object Storage Replication tools
### No Vendor Locked-In

## S3 to GCS, GCS to S3, etc

set these envs first

```shell
SOURCE_OBJECT_STORAGE_PLATFORM=s3
// available now is s3

SOURCE_AWS_REGION=<SOURCE_AWS_REGION>
SOURCE_AWS_ACCESS_KEY=<SOURCE_AWS_ACCESS_KEY>
SOURCE_AWS_SECRET_KEY=<SOURCE_AWS_SECRET_KEY>
SOURCE_AWS_BUCKET_NAME=<SOURCE_AWS_BUCKET_NAME>

DEST_OBJECT_STORAGE_PLATFORM=s3
// available now is s3

DEST_AWS_REGION=<DEST_AWS_REGION>
DEST_AWS_ACCESS_KEY=<DEST_AWS_ACCESS_KEY>
DEST_AWS_SECRET_KEY=<DEST_AWS_SECRET_KEY>
DEST_AWS_BUCKET_NAME=<DEST_AWS_BUCKET_NAME>


// available now is 'pull_all_write' or 'pull_with_date_write'
REPLICATION_SCENARIOS=pull_all_write

// this environment should set if REPLICATION_SCENARIOS is pull_with_date_write
// available options, -(some number)(metrics)
// d = days
// h = hours
// m = minutes
// s = seconds
// for examples -1d, means it will sync for 1 day data ago
START_DATE=-1d
// 
```

#How to Execute
```shell
go run main.go
```

or; compile it first
```shell
go build -o <app_name>
./<app_name>
```