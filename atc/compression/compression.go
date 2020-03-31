package compression

import (
	"io"

	"github.com/concourse/baggageclaim"
)

//go:generate counterfeiter . Compression

type Compression interface {
	NewReader(io.ReadCloser) (Reader, error)
	Encoding() baggageclaim.Encoding
}

type Reader interface {
	Read(p []byte) (int, error)
	Close() error
}
