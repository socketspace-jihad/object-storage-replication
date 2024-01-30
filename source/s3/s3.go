package s3

import (
	"errors"
	"io"
	"log"
	"os"
	"time"

	awssdk "github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/socketspace-jihad/s3-sync-replication/backend/aws"
	"github.com/socketspace-jihad/s3-sync-replication/serializer"
	"github.com/socketspace-jihad/s3-sync-replication/source"
)

type SourceS3 aws.AWSS3

func NewSourceS3() source.Source {
	sess, err := session.NewSession(&awssdk.Config{
		Region: awssdk.String(os.Getenv("SOURCE_AWS_REGION")),
		Credentials: credentials.NewStaticCredentials(
			os.Getenv("SOURCE_AWS_ACCESS_KEY"),
			os.Getenv("SOURCE_AWS_SECRET_KEY"),
			"",
		),
	})
	if err != nil {
		panic(err)
	}
	return &SourceS3{
		Session:    sess,
		BucketName: os.Getenv("SOURCE_AWS_BUCKET_NAME"),
	}
}

func (s *SourceS3) Validate() error {
	if _, ok := os.LookupEnv("SOURCE_AWS_REGION"); !ok {
		return errors.New("environment not found: SOURCE_AWS_REGION")
	}
	if _, ok := os.LookupEnv("SOURCE_AWS_ACCESS_KEY"); !ok {
		return errors.New("environment not found: SOURCE_AWS_ACCESS_KEY")
	}
	if _, ok := os.LookupEnv("SOURCE_AWS_SECRET_KEY"); !ok {
		return errors.New("environment not found: SOURCE_AWS_SECRET_KEY")
	}
	if _, ok := os.LookupEnv("SOURCE_AWS_BUCKET_NAME"); !ok {
		return errors.New("environment not found: SOURCE_AWS_BUCKET_NAME")
	}

	log.Println("SOURCE BUCKET:", os.Getenv("SOURCE_AWS_BUCKET_NAME"))
	log.Println("SOURCE REGION:", os.Getenv("SOURCE_AWS_REGION"))
	return nil
}

func (s *SourceS3) PullAll() []chan serializer.SEF {
	log.Println("SOURCE BUCKET:", s.BucketName)
	bucket := s3.New(s.Session)
	result := []chan serializer.SEF{}
	err := bucket.ListObjectsPages(&s3.ListObjectsInput{
		Bucket: awssdk.String(s.BucketName),
	}, func(data *s3.ListObjectsOutput, b bool) bool {
		for _, obj := range data.Contents {
			c := make(chan serializer.SEF)
			go func(obj *s3.Object) {
				f, err := bucket.GetObject(&s3.GetObjectInput{
					Bucket: awssdk.String(s.BucketName),
					Key:    awssdk.String(*obj.Key),
				})
				if err != nil {
					log.Println("ERROR GET OBJECT :", err)
				}
				defer f.Body.Close()
				res, err := io.ReadAll(f.Body)
				if err != nil {
					log.Println("ERROR READ BUFFER: ", err)
				}
				c <- serializer.SEF{
					Data:     res,
					Filename: *obj.Key,
				}
				close(c)
			}(obj)
			result = append(result, c)
		}
		return true
	})
	if err != nil {
		log.Println(err)
	}
	return result
}

func (s *SourceS3) PullWithDateFilter(date time.Time) []chan serializer.SEF {
	bucket := s3.New(s.Session)
	result := []chan serializer.SEF{}
	err := bucket.ListObjectsPages(&s3.ListObjectsInput{
		Bucket: awssdk.String(s.BucketName),
	}, func(data *s3.ListObjectsOutput, b bool) bool {
		for _, obj := range data.Contents {
			if obj.LastModified.After(date) {
				c := make(chan serializer.SEF)
				go func(obj *s3.Object) {
					f, err := bucket.GetObject(&s3.GetObjectInput{
						Bucket: awssdk.String(s.BucketName),
						Key:    awssdk.String(*obj.Key),
					})
					if err != nil {
						log.Println("ERROR GET OBJECT :", err)
					}
					defer f.Body.Close()
					res, err := io.ReadAll(f.Body)
					if err != nil {
						log.Println("ERROR READ BUFFER: ", err)
					}
					c <- serializer.SEF{
						Data:          res,
						Filename:      *obj.Key,
						AWSS3Metadata: f.Metadata,
					}
					close(c)
				}(obj)
				result = append(result, c)
			}
		}
		return true
	})
	if err != nil {
		panic(err)
	}
	return result
}

func (s *SourceS3) PullWithNameFilter(name string) []chan serializer.SEF {
	return nil
}

func init() {
	source.RegisterSource("s3", NewSourceS3)
}
