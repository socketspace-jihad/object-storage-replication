package aws

import "github.com/aws/aws-sdk-go/aws/session"

type AWSCreds struct {
	AccessKey     string `yaml:"access_key"`
	SecretKey     string `yaml:"secret_key"`
	DefaultRegion string `yaml:"default_region"`
}

type AWSS3 struct {
	*AWSCreds  `yaml:",inline"`
	Session    *session.Session
	BucketName string `yaml:"bucket_name"`
	Region     string `yaml:"region"`
}
