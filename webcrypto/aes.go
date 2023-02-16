package webcrypto

import (
	"bufio"
	"crypto/aes"
	"crypto/cipher"
	"encoding/hex"
	"fmt"
	"io"

	"github.com/d1str0/pkcs7"
	"github.com/golang/glog"
)

// Decrypts a file encrypted with AES (key length depends on input) in CBC block
// chaining mode and PKCS#7 padding. The provided key must be encoded in base16,
// and the first block of the input is the IV. The output is a pipe reader that
// can be used to stream the decrypted file.
func DecryptAESCBC(reader io.ReadCloser, keyb16 string) (io.ReadCloser, error) {
	iv := make([]byte, aes.BlockSize)
	if _, err := io.ReadFull(reader, iv); err != nil {
		return nil, fmt.Errorf("error reading iv from input: %w", err)
	}

	return DecryptAESCBCWithIV(reader, keyb16, iv)
}

// Just like DecryptAESCBC, but the IV is provided as an argument instead of
// read from the input stream.
func DecryptAESCBCWithIV(reader io.ReadCloser, keyb16 string, iv []byte) (io.ReadCloser, error) {
	key, err := hex.DecodeString(keyb16)
	if err != nil {
		return nil, fmt.Errorf("error decoding base16 key: %w", err)
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("error creating cipher: %w", err)
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
		}

		chunk := buffer[:n]

		// we add some dummy bytes in the end to make a full block and still try to
		// decrypt. this is non standard and can only ever happen on the last chunk.
		needsFakePadding := n%blockSize != 0
		if needsFakePadding {
			glog.Warningf("Input is not a multiple of AES block size, not padded with PKCS#7")
			fakePaddingSize := blockSize - (n % blockSize)
			chunk = buffer[:n+fakePaddingSize]
		}

		decrypter.CryptBlocks(chunk, chunk)

		if _, peekErr := reader.Peek(1); !needsFakePadding && peekErr == io.EOF {
			// this means we're on the last chunk, so handle padding
			lastBlock := chunk[len(chunk)-blockSize:]
			unpadded, err := pkcs7.Unpad(lastBlock)
			if err != nil {
				return fmt.Errorf("bad input PKCS#7 padding: %w", err)
			}

			padSize := len(lastBlock) - len(unpadded)
			chunk = chunk[:len(chunk)-padSize]
		}

		// still need to slice chunk in case we added fake padding
		if _, err := writer.Write(chunk[:n]); err != nil {
			return err
		}
	}

	return nil
}
