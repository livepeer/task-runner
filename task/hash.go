package task

import (
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"hash"
	"io"
)

type ReadHasher struct {
	size   int64
	md5    hash.Hash
	sha256 hash.Hash
	r      io.Reader
}

func NewReadHasher(r io.Reader) *ReadHasher {
	return &ReadHasher{
		md5:    md5.New(),
		sha256: sha256.New(),
		r:      r,
	}
}

func (h *ReadHasher) Read(p []byte) (int, error) {
	n, err := h.r.Read(p)
	if n > 0 {
		h.size += int64(n)
		// hashers never return errors
		h.md5.Write(p[:n])
		h.sha256.Write(p[:n])
	}
	return n, err
}

func (h *ReadHasher) FinishReader() (int64, error) {
	n, err := io.Copy(io.MultiWriter(h.md5, h.sha256), h.r)
	if n > 0 {
		h.size += n
	}
	return n, err
}

func (h *ReadHasher) Size() int64 {
	return h.size
}

func (h *ReadHasher) MD5() string {
	return hex.EncodeToString(h.md5.Sum(nil))
}

func (h *ReadHasher) SHA256() string {
	return hex.EncodeToString(h.sha256.Sum(nil))
}
