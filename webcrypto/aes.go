package webcrypto

import (
	"bufio"
	"crypto/aes"
	"crypto/cipher"
	"encoding/hex"
	"fmt"
	"io"

	"github.com/d1str0/pkcs7"
)

func DecryptAESCBC(reader io.ReadCloser, keyb16 string) (io.ReadCloser, error) {
	key, err := hex.DecodeString(keyb16)
	if err != nil {
		return nil, fmt.Errorf("error decoding base16 key: %w", err)
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("error creating cipher: %w", err)
	}

	iv := make([]byte, block.BlockSize())
	if _, err := io.ReadFull(reader, iv); err != nil {
		return nil, fmt.Errorf("error reading iv from input: %w", err)
	}

	decrypter := cipher.NewCBCDecrypter(block, iv)
	pipeReader, pipeWriter := io.Pipe()

	go func() {
		defer reader.Close()
		defer pipeWriter.Close()

		if err := decryptReaderTo(reader, pipeWriter, decrypter); err != nil {
			pipeWriter.CloseWithError(err)
		}
	}()

	return pipeReader, nil
}

func decryptReaderTo(readerRaw io.Reader, writer io.Writer, decrypter cipher.BlockMode) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
	}()

	blockSize := decrypter.BlockSize()
	buffer := make([]byte, 256*blockSize)
	reader := bufio.NewReaderSize(readerRaw, 2*len(buffer))

	for {
		n, err := io.ReadFull(reader, buffer)
		if n == 0 || err == io.EOF {
			break
		} else if err != nil && err != io.ErrUnexpectedEOF {
			// unexpected EOF is returned when input ends before the buffer size
			return err
		} else if n%blockSize != 0 {
			// this can only ever happen on the last chunk
			return fmt.Errorf("input is not a multiple of AES block size. must be padded with PKCS#7")
		}

		chunk := buffer[:n]
		decrypter.CryptBlocks(chunk, chunk)

		if _, peekErr := reader.Peek(1); peekErr == io.EOF {
			// this means we're on the last chunk, so handle padding
			lastBlock := chunk[len(chunk)-blockSize:]
			unpadded, err := pkcs7.Unpad(lastBlock)
			if err != nil {
				return fmt.Errorf("bad input PKCS#7 padding: %w", err)
			}

			padSize := len(lastBlock) - len(unpadded)
			chunk = chunk[:len(chunk)-padSize]
		}

		if _, err := writer.Write(chunk); err != nil {
			return err
		}
	}

	return nil
}
