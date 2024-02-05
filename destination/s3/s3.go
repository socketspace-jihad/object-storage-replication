package s3

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	awssdk "github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/client"
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

func upload(ctx context.Context, bucket *s3.S3, doneChan chan error, b serializer.SEF, d *DestinationS3) {
	f, err := io.ReadAll(b.Body)
	if err != nil {
		doneChan <- err
		return
	}
	length := len(f)
	if _, err := bucket.PutObjectWithContext(ctx, &s3.PutObjectInput{
		Bucket:               awssdk.String(d.BucketName),
		Key:                  awssdk.String(b.Filename),
		Body:                 awssdk.ReadSeekCloser(bytes.NewReader(f)),
		Metadata:             b.AWSS3Object.Metadata,
		ContentType:          b.AWSS3Object.ContentType,
		ContentDisposition:   b.AWSS3Object.ContentDisposition,
		ContentLanguage:      b.AWSS3Object.ContentLanguage,
		ContentEncoding:      b.AWSS3Object.ContentEncoding,
		ChecksumSHA256:       b.AWSS3Object.ChecksumSHA256,
		ChecksumAlgorithm:    b.AWSS3Object.ChecksumAlgorithm,
		ContentLength:        awssdk.Int64(int64(length)),
		ServerSideEncryption: b.AWSS3Object.ServerSideEncryption,
	}); err != nil {
		doneChan <- err
	}
	doneChan <- nil
}

func (d *DestinationS3) Write(data <-chan serializer.SEF, wg *sync.WaitGroup) error {
	log.Println("PREPARING WRITE DESTINATION..")
	bucket := s3.New(d.Session, &awssdk.Config{
		Retryer: client.DefaultRetryer{
			NumMaxRetries:    3,
			MinRetryDelay:    2 * time.Second,
			MaxRetryDelay:    5 * time.Second,
			MinThrottleDelay: 2 * time.Second,
			MaxThrottleDelay: 10 * time.Second,
		},
		HTTPClient: &http.Client{
			Timeout: 24 * time.Hour,
			Transport: &http.Transport{
				IdleConnTimeout: 300 * time.Second,
				MaxIdleConns:    5000,
			},
		},
	})
	for b := range data {
		go func(b serializer.SEF) {
			defer wg.Done()
			defer b.Body.Close()
			headCheckBucket := s3.New(d.Session, &awssdk.Config{
				Retryer: client.DefaultRetryer{
					NumMaxRetries:    3,
					MinRetryDelay:    2 * time.Second,
					MaxRetryDelay:    5 * time.Second,
					MinThrottleDelay: 2 * time.Second,
					MaxThrottleDelay: 10 * time.Second,
				},
				HTTPClient: http.DefaultClient,
			})
			_, err := headCheckBucket.HeadObject(&s3.HeadObjectInput{
				Bucket: awssdk.String(d.BucketName),
				Key:    awssdk.String(b.Filename),
			})
			if err != nil {
				if awsErr, ok := err.(awserr.Error); ok {
					switch awsErr.Code() {
					case "NotFound":
						log.Printf("Object: %v not found on destination, Writing Data\n", b.Filename)
						ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
						defer cancel()
						doneChan := make(chan error)
						go upload(ctx, bucket, doneChan, b, d)
						select {
						case <-ctx.Done():
							log.Println("UPLOAD TIMEOUT!")
						case err := <-doneChan:
							if err != nil {
								log.Println("UPLOAD ERROR", err)
							}
						}
					default:
						log.Println("NOT UPLOADING.", awsErr)
					}
				}
			}
		}(b)
	}
	return nil
}

func init() {
	destination.RegisterDestination("s3", NewDestinationS3)
}
