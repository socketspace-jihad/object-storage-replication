package serializer

// Standard Exchange Format
type SEF struct {
	Data          []byte
	Filename      string
	AWSS3Metadata map[string]*string
}
