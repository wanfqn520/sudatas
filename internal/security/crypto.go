package security

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"io"
	"math/big"
	"os"
	"path/filepath"

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

	// 生成SM4密钥（确保是16字节）
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
	// 使用 sm2.EncryptAsn1 的正确方式，添加随机数生成器
	return sm2.EncryptAsn1(cm.keyPair.PublicKey, data, rand.Reader)
}

// DecryptSM2 使用SM2解密
func (cm *CryptoManager) DecryptSM2(ciphertext []byte) ([]byte, error) {
	// 使用 sm2.DecryptAsn1 的正确方式
	return sm2.DecryptAsn1(cm.keyPair.PrivateKey, ciphertext)
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
	// 创建密钥目录
	dir := filepath.Dir(filename)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("创建密钥目录失败: %w", err)
	}

	// 保存私钥文件
	privateKeyFile := filename + ".pri"
	if err := os.WriteFile(privateKeyFile, cm.keyPair.PrivateKey.D.Bytes(), 0600); err != nil {
		return fmt.Errorf("保存私钥失败: %w", err)
	}

	// 保存SM4密钥文件
	sm4KeyFile := filename + ".sm4"
	if err := os.WriteFile(sm4KeyFile, cm.sm4Key, 0600); err != nil {
		return fmt.Errorf("保存SM4密钥失败: %w", err)
	}

	return nil
}

// LoadKeys 从文件加载密钥
func (cm *CryptoManager) LoadKeys(filename string) error {
	// 检查私钥文件
	privateKeyFile := filename + ".pri"
	if _, err := os.Stat(privateKeyFile); os.IsNotExist(err) {
		// 如果文件不存在，创建新的密钥管理器
		newCrypto, err := NewCryptoManager()
		if err != nil {
			return fmt.Errorf("创建新密钥失败: %w", err)
		}
		cm.keyPair = newCrypto.keyPair
		cm.sm4Key = newCrypto.sm4Key
		return cm.SaveKeys(filename)
	}

	// 读取私钥
	privateKeyBytes, err := os.ReadFile(privateKeyFile)
	if err != nil {
		return fmt.Errorf("读取私钥失败: %w", err)
	}

	// 重新构造私钥
	privateKey := new(sm2.PrivateKey)
	privateKey.Curve = sm2.P256Sm2()
	privateKey.D = new(big.Int).SetBytes(privateKeyBytes)
	privateKey.PublicKey.X, privateKey.PublicKey.Y = privateKey.Curve.ScalarBaseMult(privateKeyBytes)

	// 读取SM4密钥
	sm4KeyFile := filename + ".sm4"
	sm4Key, err := os.ReadFile(sm4KeyFile)
	if err != nil || len(sm4Key) != 16 {
		return fmt.Errorf("读取SM4密钥失败: %w", err)
	}

	cm.keyPair = &KeyPair{
		PrivateKey: privateKey,
		PublicKey:  &privateKey.PublicKey,
	}
	cm.sm4Key = sm4Key

	return nil
}
