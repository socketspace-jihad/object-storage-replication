## Object Storage Replication tools
### No Vendor Locked-In

## S3 to GCS, GCS to S3, etc

Set these environments
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


REPLICATION_SCENARIOS=pull_all_write
// available now is 'pull_all_write' or 'pull_with_date_write'

#How to Execute
go run main.go

or; compile it first
go build -o <app_name>
./<app_name>