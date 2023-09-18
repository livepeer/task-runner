package task

import (
	"bytes"
	"io"
	"math/rand"
	"os"
	"sync/atomic"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/joy4/format"
)

const (
	segLen               = 2 * time.Second
	maxFileSizeForMemory = 50_000_000
)

func init() {
	format.RegisterAll()
	rand.Seed(time.Now().UnixNano())
}

func readFileToMemory(r io.Reader) (io.ReadSeekCloser, error) {
	fileInMem, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	return nopCloser{bytes.NewReader(fileInMem)}, nil
}

type autodeletingFile struct {
	*os.File
}

func (adf *autodeletingFile) Reader() io.ReadSeekCloser {
	adf.Seek(0, io.SeekStart)
	return adf
}

func (adf *autodeletingFile) Close() error {
	err := adf.File.Close()
	os.Remove(adf.File.Name())
	return err
}

func getTempFile(size int64) (*os.File, error) {
	file, err := os.CreateTemp("", "transcode")
	if err != nil {
		glog.Errorf("Error creating temporary file err=%v", err)
		return nil, err
	}
	glog.Infof("Created temporary file name=%s", file.Name())
	if size > 0 {
		offset, err := file.Seek(size, io.SeekStart)
		if err != nil || offset != size {
			os.Remove(file.Name())
			glog.Errorf("Error creating temporary file name=%s with size=%d offset=%d err=%v", file.Name(), size, offset, err)
			return nil, err
		}
		file.Seek(0, io.SeekStart)
	}
	return file, nil
}

func readFile(name string, sizePtr *int64, content io.Reader) (io.ReadSeekCloser, error) {
	var size int64
	if sizePtr != nil {
		size = *sizePtr
	}
	glog.Infof("Source file name=%s size=%d", name, size)
	if size > 0 && size < maxFileSizeForMemory {
		// use memory
		return readFileToMemory(content)
	}
	if file, err := getTempFile(size); err != nil {
		return readFileToMemory(content)
	} else {
		if _, err = file.ReadFrom(content); err != nil {
			file.Close()
			os.Remove(file.Name())
			return nil, err
		}
		file.Seek(0, io.SeekStart)
		return &autodeletingFile{file}, nil
	}
}

type Accumulator struct {
	size uint64
}

func NewAccumulator() *Accumulator {
	return &Accumulator{}
}

func (a *Accumulator) Size() uint64 {
	return atomic.LoadUint64(&a.size)
}

func (a *Accumulator) Accumulate(size uint64) {
	atomic.AddUint64(&a.size, size)
}

type nopCloser struct {
	*bytes.Reader
}

func (nopCloser) Close() error { return nil }
