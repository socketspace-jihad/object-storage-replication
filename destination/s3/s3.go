package s3

import (
	"bytes"
	"errors"
	"log"
	"os"

	awssdk "github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/socketspace-jihad/s3-sync-replication/backend/aws"
	"github.com/socketspace-jihad/s3-sync-replication/destination"
	"github.com/socketspace-jihad/s3-sync-replication/serializer"
)

type DestinationS3 aws.AWSS3

func NewDestinationS3() destination.Destination {
	sess, err := session.NewSession(&awssdk.Config{
		Region: awssdk.String(os.Getenv("DEST_AWS_REGION")),
		Credentials: credentials.NewStaticCredentials(
			os.Getenv("DEST_AWS_ACCESS_KEY"),
			os.Getenv("DEST_AWS_SECRET_KEY"),
			"",
		),
	})
	if err != nil {
		panic(err)
	}
	return &DestinationS3{
		Session:    sess,
		BucketName: os.Getenv("DEST_AWS_BUCKET_NAME"),
	}
}

func (d *DestinationS3) WriteOverride(data chan serializer.SEF) error {
	return nil
}

func (d *DestinationS3) Validate() error {
	if _, ok := os.LookupEnv("DEST_AWS_REGION"); !ok {
		return errors.New("environment not found: DEST_AWS_REGION")
	}
	if _, ok := os.LookupEnv("DEST_AWS_ACCESS_KEY"); !ok {
		return errors.New("environment not found: DEST_AWS_ACCESS_KEY")
	}
	if _, ok := os.LookupEnv("DEST_AWS_SECRET_KEY"); !ok {
		return errors.New("environment not found: DEST_AWS_SECRET_KEY")
	}
	if _, ok := os.LookupEnv("DEST_AWS_BUCKET_NAME"); !ok {
		return errors.New("environment not found: DEST_AWS_BUCKET_NAME")
	}
	log.Println("DESTINATION BUCKET:", os.Getenv("DEST_AWS_BUCKET_NAME"))
	log.Println("DESTINATION REGION:", os.Getenv("DEST_AWS_REGION"))
	return nil
}

func (d *DestinationS3) Write(data chan serializer.SEF) error {
	bucket := s3.New(d.Session)
	b := <-data
	_, err := bucket.HeadObject(&s3.HeadObjectInput{
		Bucket: awssdk.String(d.BucketName),
		Key:    awssdk.String(b.Filename),
	})
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			switch awsErr.Code() {
			case "NotFound":
				log.Printf("Object: %v not found on destination, Writing Data\n", b.Filename)
				if _, err := bucket.PutObject(&s3.PutObjectInput{
					Bucket:   awssdk.String(d.BucketName),
					Key:      awssdk.String(b.Filename),
					Body:     awssdk.ReadSeekCloser(bytes.NewReader(b.Data)),
					Metadata: b.AWSS3Metadata,
				}); err != nil {
					log.Println(err.Error())
				}
			default:
				log.Println("NOT UPLOADING.", awsErr.Code())
			}
		}
	}
	return nil
}

func init() {
	destination.RegisterDestination("s3", NewDestinationS3)
}
