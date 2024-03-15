package serializer

import (
	"io"

	"github.com/aws/aws-sdk-go/service/s3"
)

type AWSS3Object struct {
	Body                 io.ReadCloser
	Metadata             map[string]*string
	ContentLength        *int64
	ContentType          *string
	ContentDisposition   *string
	ContentLanguage      *string
	ContentEncoding      *string
	ChecksumSHA256       *string
	ContentMD5           *string
	ChecksumAlgorithm    *string
	ServerSideEncryption *string
	ACL                  *s3.GetBucketAclOutput
}

// Standard Exchange Format
type SEF struct {
	Data     []byte
	Filename string
	*AWSS3Object
}
