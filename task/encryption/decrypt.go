package encryption

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/hex"
	"fmt"
	"io"
)

func DecryptAES256CBCReader(reader io.ReadCloser, keyb16 string) (io.ReadCloser, error) {
	key, err := hex.DecodeString(keyb16)
	if err != nil {
		return nil, fmt.Errorf("error decoding base16 key: %w", err)
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("error creating cipher: %w", err)
	}

	iv := make([]byte, aes.BlockSize)
	if _, err := reader.Read(iv); err != nil {
		return nil, fmt.Errorf("error reading iv from input: %w", err)
	}

	stream := cipher.NewCBCDecrypter(block, iv)
	pipeReader, pipeWriter := io.Pipe()

	go func() {
		defer reader.Close()
		defer pipeWriter.Close()
		defer func() {
			if r := recover(); r != nil {
				pipeWriter.CloseWithError(fmt.Errorf("panic: %v", r))
			}
		}()

		buffer := make([]byte, 256*aes.BlockSize)
		for {
			n, err := io.ReadFull(reader, buffer)
			if n == 0 {
				break
			} else if err != nil && err != io.ErrUnexpectedEOF {
				pipeWriter.CloseWithError(err)
				return
			}

			// padding only happens on the last block and we just ignore the extra bytes
			padSize := n % aes.BlockSize
			paddedSize := n + padSize
			stream.CryptBlocks(buffer[:paddedSize], buffer[:paddedSize])

			if _, err := pipeWriter.Write(buffer[:n]); err != nil {
				pipeWriter.CloseWithError(err)
				return
			}
		}
	}()

	return pipeReader, nil
}
