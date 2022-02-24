package lib

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
)

func AESDecrypt(keyHex string, aesIVHex string, ciphertext []byte) string {
	key, _ := hex.DecodeString(keyHex)

	block, err := aes.NewCipher(key)
	if err != nil {
		log.Error(string(ciphertext), err)
		return ""
	}

	iv, err := hex.DecodeString(aesIVHex)
	if err != nil {
		log.Error(string(ciphertext), err)
		return ""
	}

	mode := cipher.NewCBCDecrypter(block, iv)
	mode.CryptBlocks(ciphertext, ciphertext)

	return fmt.Sprintf("%s", ciphertext)
}

func AESEncrypt(keyHex string, aesIVHex string, text string) []byte {
	plaintext := []byte(text)
	key, _ := hex.DecodeString(keyHex)

	if len(plaintext)%aes.BlockSize != 0 {
		log.Warn(text, "plaintext is not a multiple of the block size")
		return []byte{}
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		log.Error(text, err)
		return []byte{}
	}

	ciphertext := make([]byte, len(plaintext))
	iv, err := hex.DecodeString(aesIVHex)
	if err != nil {
		log.Error(text, err)
		return []byte{}
	}
	mode := cipher.NewCBCEncrypter(block, iv)

	mode.CryptBlocks(ciphertext, plaintext)

	return ciphertext
}

func CriptoPadding(text string, blockSize int, paddingChar string) string {
	return text + strings.Repeat(paddingChar, blockSize-len(text)%blockSize)
}

// chksum

func ValidCheckSumAdler32(data string, chk string, numBytes int) bool {
	chkStr := Lpad(chk, "0", numBytes)
	calcChk := ChkSumAdler32([]byte(data), len(data), numBytes)
	if chkStr != calcChk {
		log.Warn("Checksum err:", chkStr, "!=", calcChk)
	}
	return chkStr == calcChk
}

func ChkSumAdler32(buffer []byte, bufferSize int, numBytes int) string {
	modAdler := 65521
	a := 1
	b := 0
	for x := 0; x < bufferSize; x++ {
		a = (a + int(buffer[x])) % modAdler
		b = (b + a) % modAdler
	}
	b = b << 16
	retInt := b | a
	retStr := strings.ToUpper(strconv.FormatInt(int64(retInt), 16))
	retStr = Lpad(retStr, "0", numBytes)
	return retStr[len(retStr)-numBytes:]
}

// ECB

type ecb struct {
	b         cipher.Block
	blockSize int
}

func newECB(b cipher.Block) *ecb {
	return &ecb{
		b:         b,
		blockSize: b.BlockSize(),
	}
}

type ecbEncrypter ecb

func NewECBEncrypter(b cipher.Block) cipher.BlockMode {
	return (*ecbEncrypter)(newECB(b))
}

func (x *ecbEncrypter) BlockSize() int { return x.blockSize }

func (x *ecbEncrypter) CryptBlocks(dst, src []byte) {
	if len(src)%x.blockSize != 0 {
		panic("crypto/cipher: input not full blocks")
	}
	if len(dst) < len(src) {
		panic("crypto/cipher: output smaller than input")
	}
	for len(src) > 0 {
		x.b.Encrypt(dst, src[:x.blockSize])
		src = src[x.blockSize:]
		dst = dst[x.blockSize:]
	}
}

type ecbDecrypter ecb

func NewECBDecrypter(b cipher.Block) cipher.BlockMode {
	return (*ecbDecrypter)(newECB(b))
}

func (x *ecbDecrypter) BlockSize() int { return x.blockSize }

func (x *ecbDecrypter) CryptBlocks(dst, src []byte) {
	if len(src)%x.blockSize != 0 {
		panic("crypto/cipher: input not full blocks")
	}
	if len(dst) < len(src) {
		panic("crypto/cipher: output smaller than input")
	}
	for len(src) > 0 {
		x.b.Decrypt(dst, src[:x.blockSize])
		src = src[x.blockSize:]
		dst = dst[x.blockSize:]
	}
}

func AESECBDecrypt(keyHex string, ciphertext []byte) []byte {
	key, _ := hex.DecodeString(keyHex)

	block, err := aes.NewCipher(key)
	if err != nil {
		log.Error(string(ciphertext), err)
		return []byte{}
	}

	mode := NewECBDecrypter(block)
	mode.CryptBlocks(ciphertext, ciphertext)

	return ciphertext
}

func AESECBEncrypt(keyHex string, text []byte) []byte {
	key, _ := hex.DecodeString(keyHex)

	if len(text)%aes.BlockSize != 0 {
		log.Warn(text, "text is not a multiple of the block size")
		return []byte{}
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		log.Error(text, err)
		return []byte{}
	}

	ciphertext := make([]byte, len(text))

	mode := NewECBEncrypter(block)

	mode.CryptBlocks(ciphertext, text)

	return ciphertext
}
