package encryption

import (
	"bufio"
	"crypto/aes"
	"crypto/cipher"
	"encoding/hex"
	"fmt"
	"io"

	"github.com/d1str0/pkcs7"
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

	decrypter := cipher.NewCBCDecrypter(block, iv)
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
		bufReader := bufio.NewReaderSize(reader, 2*len(buffer))
		for {
			n, err := io.ReadFull(bufReader, buffer)
			if n == 0 || err == io.EOF {
				break
			} else if err != nil && err != io.ErrUnexpectedEOF {
				// unexpected EOF is returned when input ends before the buffer size
				pipeWriter.CloseWithError(err)
				return
			} else if n%aes.BlockSize != 0 {
				// this can only ever happen on the last chunk
				err := fmt.Errorf("input is not a multiple of AES block size. must be padded with PKCS#7")
				pipeWriter.CloseWithError(err)
				return
			}

			chunk := buffer[:n]
			decrypter.CryptBlocks(chunk, chunk)

			if _, peekErr := bufReader.Peek(1); peekErr == io.EOF {
				// this means we're on the last chunk, so handle padding
				lastBlock := chunk[len(chunk)-aes.BlockSize:]
				unpadded, err := pkcs7.Unpad(lastBlock)
				if err != nil {
					pipeWriter.CloseWithError(fmt.Errorf("bad input PKCS#7 padding: %w", err))
					return
				}

				padSize := len(lastBlock) - len(unpadded)
				chunk = chunk[:len(chunk)-padSize]
			}

			if _, err := pipeWriter.Write(chunk); err != nil {
				pipeWriter.CloseWithError(err)
				return
			}
		}
	}()

	return pipeReader, nil
}
