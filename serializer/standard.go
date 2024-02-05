package serializer

import (
	"io"
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
}

// Standard Exchange Format
type SEF struct {
	Data     []byte
	Filename string
	*AWSS3Object
}
