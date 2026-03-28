package s3

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/snithish/mockbucket/internal/core"
)

func decodeStreamingBody(r *http.Request) (io.ReadCloser, error) {
	if !usesAWSChunkedEncoding(r) {
		return r.Body, nil
	}
	return &awsChunkedReader{
		body:   r.Body,
		reader: bufio.NewReader(r.Body),
	}, nil
}

func usesAWSChunkedEncoding(r *http.Request) bool {
	if strings.HasPrefix(r.Header.Get("X-Amz-Content-Sha256"), "STREAMING-AWS4-HMAC-SHA256-PAYLOAD") {
		return true
	}
	for _, encoding := range r.Header.Values("Content-Encoding") {
		for _, part := range strings.Split(encoding, ",") {
			if strings.EqualFold(strings.TrimSpace(part), "aws-chunked") {
				return true
			}
		}
	}
	return false
}

type awsChunkedReader struct {
	body      io.ReadCloser
	reader    *bufio.Reader
	remaining int64
	done      bool
}

func (r *awsChunkedReader) Read(p []byte) (int, error) {
	if r.done {
		return 0, io.EOF
	}
	if r.remaining == 0 {
		size, err := r.readChunkHeader()
		if err != nil {
			return 0, err
		}
		if size == 0 {
			if err := r.readTrailers(); err != nil {
				return 0, err
			}
			r.done = true
			return 0, io.EOF
		}
		r.remaining = size
	}

	if int64(len(p)) > r.remaining {
		p = p[:r.remaining]
	}
	n, err := r.reader.Read(p)
	r.remaining -= int64(n)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return n, core.ErrInvalidArgument
		}
		return n, err
	}
	if r.remaining == 0 {
		if err := r.expectCRLF(); err != nil {
			return n, err
		}
	}
	return n, nil
}

func (r *awsChunkedReader) Close() error {
	return r.body.Close()
}

func (r *awsChunkedReader) readChunkHeader() (int64, error) {
	line, err := r.reader.ReadString('\n')
	if err != nil {
		if errors.Is(err, io.EOF) {
			return 0, core.ErrInvalidArgument
		}
		return 0, err
	}
	line = strings.TrimSuffix(line, "\n")
	line = strings.TrimSuffix(line, "\r")
	sizeField := line
	if idx := strings.IndexByte(sizeField, ';'); idx >= 0 {
		sizeField = sizeField[:idx]
	}
	sizeField = strings.TrimSpace(sizeField)
	if sizeField == "" {
		return 0, core.ErrInvalidArgument
	}
	size, err := strconv.ParseInt(sizeField, 16, 64)
	if err != nil || size < 0 {
		return 0, core.ErrInvalidArgument
	}
	return size, nil
}

func (r *awsChunkedReader) readTrailers() error {
	for {
		line, err := r.reader.ReadString('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				return core.ErrInvalidArgument
			}
			return err
		}
		if line == "\r\n" {
			return nil
		}
		if !strings.HasSuffix(line, "\r\n") {
			return fmt.Errorf("invalid aws-chunked trailer: %w", core.ErrInvalidArgument)
		}
	}
}

func (r *awsChunkedReader) expectCRLF() error {
	line, err := r.reader.ReadString('\n')
	if err != nil {
		if errors.Is(err, io.EOF) {
			return core.ErrInvalidArgument
		}
		return err
	}
	if line != "\r\n" {
		return core.ErrInvalidArgument
	}
	return nil
}
