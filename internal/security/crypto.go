package security

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/tjfoc/gmsm/sm2"
	"github.com/tjfoc/gmsm/sm4"
)

// KeyPair SM2密钥对
type KeyPair struct {
	PrivateKey *sm2.PrivateKey
	PublicKey  *sm2.PublicKey
}

// CryptoManager 加密管理器
type CryptoManager struct {
	keyPair *KeyPair
	sm4Key  []byte
}

// NewCryptoManager 创建新的加密管理器
func NewCryptoManager() (*CryptoManager, error) {
	// 生成SM2密钥对
	privateKey, err := sm2.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("生成SM2密钥对失败: %w", err)
	}

	// 生成SM4密钥
	sm4Key := make([]byte, 16)
	if _, err := io.ReadFull(rand.Reader, sm4Key); err != nil {
		return nil, fmt.Errorf("生成SM4密钥失败: %w", err)
	}

	return &CryptoManager{
		keyPair: &KeyPair{
			PrivateKey: privateKey,
			PublicKey:  &privateKey.PublicKey,
		},
		sm4Key: sm4Key,
	}, nil
}

// EncryptSM2 使用SM2加密
func (cm *CryptoManager) EncryptSM2(data []byte) ([]byte, error) {
	return sm2.Encrypt(rand.Reader, cm.keyPair.PublicKey, data, nil)
}

// DecryptSM2 使用SM2解密
func (cm *CryptoManager) DecryptSM2(ciphertext []byte) ([]byte, error) {
	return sm2.Decrypt(cm.keyPair.PrivateKey, ciphertext)
}

// EncryptSM4 使用SM4加密
func (cm *CryptoManager) EncryptSM4(data []byte) ([]byte, error) {
	block, err := sm4.NewCipher(cm.sm4Key)
	if err != nil {
		return nil, err
	}

	// 添加填充
	padding := block.BlockSize() - len(data)%block.BlockSize()
	padtext := bytes.Repeat([]byte{byte(padding)}, padding)
	data = append(data, padtext...)

	// 加密
	ciphertext := make([]byte, len(data))
	block.Encrypt(ciphertext, data)
	return ciphertext, nil
}

// DecryptSM4 使用SM4解密
func (cm *CryptoManager) DecryptSM4(ciphertext []byte) ([]byte, error) {
	block, err := sm4.NewCipher(cm.sm4Key)
	if err != nil {
		return nil, err
	}

	// 解密
	plaintext := make([]byte, len(ciphertext))
	block.Decrypt(plaintext, ciphertext)

	// 去除填充
	padding := int(plaintext[len(plaintext)-1])
	return plaintext[:len(plaintext)-padding], nil
}

// SaveKeys 保存密钥到文件
func (cm *CryptoManager) SaveKeys(filename string) error {
	var buf bytes.Buffer

	// 保存SM2私钥
	privateKeyBytes, err := cm.keyPair.PrivateKey.GetRawBytes()
	if err != nil {
		return err
	}
	binary.Write(&buf, binary.BigEndian, uint32(len(privateKeyBytes)))
	buf.Write(privateKeyBytes)

	// 保存SM4密钥
	binary.Write(&buf, binary.BigEndian, uint32(len(cm.sm4Key)))
	buf.Write(cm.sm4Key)

	// 加密整个buffer并保存
	encrypted, err := cm.EncryptSM2(buf.Bytes())
	if err != nil {
		return err
	}

	return os.WriteFile(filename, encrypted, 0600)
}

// LoadKeys 从文件加载密钥
func (cm *CryptoManager) LoadKeys(filename string) error {
	data, err := os.ReadFile(filename)
	if err != nil {
		return err
	}

	// 解密数据
	decrypted, err := cm.DecryptSM2(data)
	if err != nil {
		return err
	}

	buf := bytes.NewReader(decrypted)

	// 读取SM2私钥
	var privateKeyLen uint32
	binary.Read(buf, binary.BigEndian, &privateKeyLen)
	privateKeyBytes := make([]byte, privateKeyLen)
	buf.Read(privateKeyBytes)
	cm.keyPair.PrivateKey, err = sm2.RawBytesToPrivateKey(privateKeyBytes)
	if err != nil {
		return err
	}
	cm.keyPair.PublicKey = &cm.keyPair.PrivateKey.PublicKey

	// 读取SM4密钥
	var sm4KeyLen uint32
	binary.Read(buf, binary.BigEndian, &sm4KeyLen)
	cm.sm4Key = make([]byte, sm4KeyLen)
	buf.Read(cm.sm4Key)

	return nil
}
