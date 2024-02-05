package s3

import (
	"errors"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	awssdk "github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/client"
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

func (s *SourceS3) PullAll(comm chan serializer.SEF, wg *sync.WaitGroup) []chan serializer.SEF {
	log.Println("PULL ALL SCENARIOS")
	bucket := s3.New(s.Session, &awssdk.Config{
		Retryer: client.DefaultRetryer{
			NumMaxRetries:    3,
			MinRetryDelay:    5 * time.Second,
			MaxRetryDelay:    10 * time.Second,
			MinThrottleDelay: 5 * time.Second,
			MaxThrottleDelay: 10 * time.Second,
		},
		HTTPClient: &http.Client{
			Timeout: 24 * time.Hour,
			Transport: &http.Transport{
				// IdleConnTimeout: 20 * time.Second,
				MaxIdleConns: 5000,
			},
		},
	})
	objectBucket := s3.New(s.Session, &awssdk.Config{
		Retryer: client.DefaultRetryer{
			NumMaxRetries:    3,
			MinRetryDelay:    5 * time.Second,
			MaxRetryDelay:    10 * time.Second,
			MinThrottleDelay: 5 * time.Second,
			MaxThrottleDelay: 10 * time.Second,
		},
		HTTPClient: &http.Client{
			Timeout: 24 * time.Hour,
			Transport: &http.Transport{
				// IdleConnTimeout: 20 * time.Second,
				MaxIdleConns:        5000,
				MaxIdleConnsPerHost: 5000,
			},
		},
	})
	result := []chan serializer.SEF{}
	err := bucket.ListObjectsPages(&s3.ListObjectsInput{
		Bucket: awssdk.String(s.BucketName),
	}, func(data *s3.ListObjectsOutput, b bool) bool {
		wg.Add(len(data.Contents))
		go func() {
			for _, obj := range data.Contents {
				go func(obj *s3.Object) {
					f, err := objectBucket.GetObject(&s3.GetObjectInput{
						Bucket: awssdk.String(s.BucketName),
						Key:    awssdk.String(*obj.Key),
					})
					if err != nil {
						log.Println("ERROR GET OBJECT :", err)
						f.Body.Close()
						wg.Done()
						return
					}
					comm <- serializer.SEF{
						Filename: *obj.Key,
						AWSS3Object: &serializer.AWSS3Object{
							Body:                 f.Body,
							Metadata:             f.Metadata,
							ContentLength:        f.ContentLength,
							ContentType:          f.ContentType,
							ChecksumSHA256:       f.ChecksumSHA256,
							ContentDisposition:   f.ContentDisposition,
							ContentLanguage:      f.ContentLanguage,
							ContentEncoding:      f.ContentEncoding,
							ChecksumAlgorithm:    f.ChecksumSHA256,
							ServerSideEncryption: f.ServerSideEncryption,
						},
					}
				}(obj)
			}
			log.Println("GRAB:", len(data.Contents))
		}()
		return true
	})
	if err != nil {
		log.Println(err)
	}
	return result
}

func (s *SourceS3) PullWithPrefix(comm chan serializer.SEF, wg *sync.WaitGroup, prefix string) []chan serializer.SEF {
	log.Println("PULL WITH PREFIX SCENARIOS")
	bucket := s3.New(s.Session, &awssdk.Config{
		Retryer: client.DefaultRetryer{
			NumMaxRetries:    3,
			MinRetryDelay:    5 * time.Second,
			MaxRetryDelay:    10 * time.Second,
			MinThrottleDelay: 5 * time.Second,
			MaxThrottleDelay: 10 * time.Second,
		},
		HTTPClient: &http.Client{
			Timeout: 24 * time.Hour,
			Transport: &http.Transport{
				// IdleConnTimeout: 20 * time.Second,
				MaxIdleConns: 5000,
			},
		},
	})
	objectBucket := s3.New(s.Session, &awssdk.Config{
		Retryer: client.DefaultRetryer{
			NumMaxRetries:    3,
			MinRetryDelay:    5 * time.Second,
			MaxRetryDelay:    10 * time.Second,
			MinThrottleDelay: 5 * time.Second,
			MaxThrottleDelay: 10 * time.Second,
		},
		HTTPClient: &http.Client{
			Timeout: 24 * time.Hour,
			Transport: &http.Transport{
				// IdleConnTimeout: 20 * time.Second,
				MaxIdleConns:        5000,
				MaxIdleConnsPerHost: 5000,
			},
		},
	})
	result := []chan serializer.SEF{}
	err := bucket.ListObjectsPages(&s3.ListObjectsInput{
		Bucket: awssdk.String(s.BucketName),
		Prefix: awssdk.String(prefix),
	}, func(data *s3.ListObjectsOutput, b bool) bool {
		wg.Add(len(data.Contents))
		go func() {
			for _, obj := range data.Contents {
				go func(obj *s3.Object) {
					f, err := objectBucket.GetObject(&s3.GetObjectInput{
						Bucket: awssdk.String(s.BucketName),
						Key:    awssdk.String(*obj.Key),
					})
					if err != nil {
						log.Println("ERROR GET OBJECT :", err)
						f.Body.Close()
						wg.Done()
						return
					}
					comm <- serializer.SEF{
						Filename: *obj.Key,
						AWSS3Object: &serializer.AWSS3Object{
							Body:                 f.Body,
							Metadata:             f.Metadata,
							ContentLength:        f.ContentLength,
							ContentType:          f.ContentType,
							ChecksumSHA256:       f.ChecksumSHA256,
							ContentDisposition:   f.ContentDisposition,
							ContentLanguage:      f.ContentLanguage,
							ContentEncoding:      f.ContentEncoding,
							ChecksumAlgorithm:    f.ChecksumSHA256,
							ServerSideEncryption: f.ServerSideEncryption,
						},
					}
				}(obj)
			}
			log.Println("GRAB:", len(data.Contents))
		}()
		return true
	})
	if err != nil {
		log.Println(err)
	}
	return result
}

func (s *SourceS3) PullWithDateFilter(comm chan serializer.SEF, wg *sync.WaitGroup, date time.Time) []chan serializer.SEF {
	log.Println("PULL WITH DATE FILTER SCENARIOS")
	bucket := s3.New(s.Session, &awssdk.Config{
		Retryer: client.DefaultRetryer{
			NumMaxRetries:    3,
			MinRetryDelay:    5 * time.Second,
			MaxRetryDelay:    10 * time.Second,
			MinThrottleDelay: 5 * time.Second,
			MaxThrottleDelay: 10 * time.Second,
		},
		HTTPClient: &http.Client{
			Timeout: 24 * time.Hour,
			Transport: &http.Transport{
				// IdleConnTimeout: 20 * time.Second,
				MaxIdleConns: 5000,
			},
		},
	})
	objectBucket := s3.New(s.Session, &awssdk.Config{
		Retryer: client.DefaultRetryer{
			NumMaxRetries:    3,
			MinRetryDelay:    5 * time.Second,
			MaxRetryDelay:    10 * time.Second,
			MinThrottleDelay: 5 * time.Second,
			MaxThrottleDelay: 10 * time.Second,
		},
		HTTPClient: &http.Client{
			Timeout: 24 * time.Hour,
			Transport: &http.Transport{
				// IdleConnTimeout: 20 * time.Second,
				MaxIdleConns:        5000,
				MaxIdleConnsPerHost: 5000,
			},
		},
	})
	result := []chan serializer.SEF{}
	err := bucket.ListObjectsPages(&s3.ListObjectsInput{
		Bucket: awssdk.String(s.BucketName),
	}, func(data *s3.ListObjectsOutput, b bool) bool {
		wg.Add(len(data.Contents))
		go func() {
			for _, obj := range data.Contents {
				if obj.LastModified.After(date) {
					go func(obj *s3.Object) {
						f, err := objectBucket.GetObject(&s3.GetObjectInput{
							Bucket: awssdk.String(s.BucketName),
							Key:    awssdk.String(*obj.Key),
						})
						if err != nil {
							log.Println("ERROR GET OBJECT :", err)
							f.Body.Close()
							wg.Done()
							return
						}
						comm <- serializer.SEF{
							Filename: *obj.Key,
							AWSS3Object: &serializer.AWSS3Object{
								Body:                 f.Body,
								Metadata:             f.Metadata,
								ContentLength:        f.ContentLength,
								ContentType:          f.ContentType,
								ChecksumSHA256:       f.ChecksumSHA256,
								ContentDisposition:   f.ContentDisposition,
								ContentLanguage:      f.ContentLanguage,
								ContentEncoding:      f.ContentEncoding,
								ChecksumAlgorithm:    f.ChecksumSHA256,
								ServerSideEncryption: f.ServerSideEncryption,
							},
						}
					}(obj)
				} else {
					wg.Done()
				}
			}
			log.Println("GRAB:", len(data.Contents))
		}()
		return true
	})
	if err != nil {
		log.Println(err)
	}
	return result
}

func (s *SourceS3) PullWithNameFilter(name string) []chan serializer.SEF {
	return nil
}

func init() {
	source.RegisterSource("s3", NewSourceS3)
}
