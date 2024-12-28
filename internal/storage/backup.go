package storage

import (
	"archive/tar"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// BackupInfo 备份信息
type BackupInfo struct {
	ID             string    `json:"id"`
	CollectionName string    `json:"collection_name"`
	DatabaseName   string    `json:"database_name,omitempty"` // 空表示整个集合
	Type           string    `json:"type"`                    // full/incremental
	Created        time.Time `json:"created"`
	Size           int64     `json:"size"`
	Status         string    `json:"status"`
	Description    string    `json:"description"`
}

// BackupManager 备份管理器
type BackupManager struct {
	backupDir string
	engine    *Engine
}

// NewBackupManager 创建备份管理器
func NewBackupManager(backupDir string, engine *Engine) (*BackupManager, error) {
	if err := os.MkdirAll(backupDir, 0755); err != nil {
		return nil, fmt.Errorf("创建备份目录失败: %w", err)
	}

	return &BackupManager{
		backupDir: backupDir,
		engine:    engine,
	}, nil
}

// BackupCollection 备份整个集合
func (bm *BackupManager) BackupCollection(collectionName, description string) (*BackupInfo, error) {
	collection, err := bm.engine.GetCollection(collectionName)
	if err != nil {
		return nil, err
	}

	backupID := fmt.Sprintf("%s_%s", collectionName, time.Now().Format("20060102150405"))
	backupPath := filepath.Join(bm.backupDir, backupID+".tar.gz")

	info := &BackupInfo{
		ID:             backupID,
		CollectionName: collectionName,
		Type:           "full",
		Created:        time.Now(),
		Status:         "in_progress",
		Description:    description,
	}

	// 创建备份文件
	file, err := os.Create(backupPath)
	if err != nil {
		return nil, fmt.Errorf("创建备份文件失败: %w", err)
	}
	defer file.Close()

	// 创建gzip写入器
	gw := gzip.NewWriter(file)
	defer gw.Close()

	// 创建tar写入器
	tw := tar.NewWriter(gw)
	defer tw.Close()

	// 保存集合元数据
	if err := bm.backupMetadata(tw, collection); err != nil {
		return nil, err
	}

	// 备份所有数据库文件
	if err := bm.backupDatabases(tw, collection); err != nil {
		return nil, err
	}

	// 更新备份信息
	info.Status = "completed"
	info.Size, _ = file.Seek(0, io.SeekCurrent)

	// 保存备份信息
	if err := bm.saveBackupInfo(info); err != nil {
		return nil, err
	}

	return info, nil
}

// RestoreCollection 从备份恢复集合
func (bm *BackupManager) RestoreCollection(backupID string) error {
	// 读取备份信息
	info, err := bm.loadBackupInfo(backupID)
	if err != nil {
		return err
	}

	backupPath := filepath.Join(bm.backupDir, backupID+".tar.gz")
	file, err := os.Open(backupPath)
	if err != nil {
		return fmt.Errorf("打开备份文件失败: %w", err)
	}
	defer file.Close()

	// 创建gzip读取器
	gr, err := gzip.NewReader(file)
	if err != nil {
		return fmt.Errorf("解压备份文件失败: %w", err)
	}
	defer gr.Close()

	// 创建tar读取器
	tr := tar.NewReader(gr)

	// 创建临时恢复目录
	tempDir := filepath.Join(bm.backupDir, "restore_"+backupID)
	if err := os.MkdirAll(tempDir, 0755); err != nil {
		return err
	}
	defer os.RemoveAll(tempDir)

	// 解压文件
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("读取备份文件失败: %w", err)
		}

		target := filepath.Join(tempDir, header.Name)

		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0755); err != nil {
				return err
			}
		case tar.TypeReg:
			dir := filepath.Dir(target)
			if err := os.MkdirAll(dir, 0755); err != nil {
				return err
			}

			f, err := os.OpenFile(target, os.O_CREATE|os.O_RDWR, os.FileMode(header.Mode))
			if err != nil {
				return err
			}
			if _, err := io.Copy(f, tr); err != nil {
				f.Close()
				return err
			}
			f.Close()
		}
	}

	// 删除现有集合
	if err := bm.engine.DeleteCollection(info.CollectionName); err != nil && !os.IsNotExist(err) {
		return err
	}

	// 移动恢复的文件到目标位置
	collectionPath := filepath.Join(bm.engine.dataDir, info.CollectionName)
	if err := os.Rename(filepath.Join(tempDir, info.CollectionName), collectionPath); err != nil {
		return fmt.Errorf("恢复文件失败: %w", err)
	}

	// 重新加载集合
	if err := bm.engine.collections.loadCollections(); err != nil {
		return fmt.Errorf("重新加载集合失败: %w", err)
	}

	return nil
}

// backupMetadata 备份元数据
func (bm *BackupManager) backupMetadata(tw *tar.Writer, collection *Collection) error {
	metaFile := filepath.Join(collection.basePath, "meta.json")
	return bm.addFileToTar(tw, metaFile, filepath.Join(collection.Name, "meta.json"))
}

// backupDatabases 备份数据库文件
func (bm *BackupManager) backupDatabases(tw *tar.Writer, collection *Collection) error {
	return filepath.Walk(collection.basePath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// 跳过集合根目录
		if path == collection.basePath {
			return nil
		}

		// 计算相对路径
		relPath, err := filepath.Rel(filepath.Dir(collection.basePath), path)
		if err != nil {
			return err
		}

		if info.IsDir() {
			// 添加目录
			header := &tar.Header{
				Name:     relPath,
				Mode:     0755,
				ModTime:  info.ModTime(),
				Typeflag: tar.TypeDir,
			}
			if err := tw.WriteHeader(header); err != nil {
				return err
			}
		} else {
			// 添加文件
			return bm.addFileToTar(tw, path, relPath)
		}

		return nil
	})
}

// addFileToTar 添加文件到tar包
func (bm *BackupManager) addFileToTar(tw *tar.Writer, src, dest string) error {
	file, err := os.Open(src)
	if err != nil {
		return err
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return err
	}

	header := &tar.Header{
		Name:    dest,
		Size:    info.Size(),
		Mode:    int64(info.Mode()),
		ModTime: info.ModTime(),
	}

	if err := tw.WriteHeader(header); err != nil {
		return err
	}

	_, err = io.Copy(tw, file)
	return err
}

// saveBackupInfo 保存备份信息
func (bm *BackupManager) saveBackupInfo(info *BackupInfo) error {
	infoFile := filepath.Join(bm.backupDir, info.ID+".json")
	data, err := json.MarshalIndent(info, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(infoFile, data, 0644)
}

// loadBackupInfo 加载备份信息
func (bm *BackupManager) loadBackupInfo(backupID string) (*BackupInfo, error) {
	infoFile := filepath.Join(bm.backupDir, backupID+".json")
	data, err := os.ReadFile(infoFile)
	if err != nil {
		return nil, err
	}

	var info BackupInfo
	if err := json.Unmarshal(data, &info); err != nil {
		return nil, err
	}

	return &info, nil
}

// ListBackups 列出所有备份
func (bm *BackupManager) ListBackups() ([]*BackupInfo, error) {
	entries, err := os.ReadDir(bm.backupDir)
	if err != nil {
		return nil, err
	}

	var backups []*BackupInfo
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}

		backupID := strings.TrimSuffix(entry.Name(), ".json")
		info, err := bm.loadBackupInfo(backupID)
		if err != nil {
			continue
		}

		backups = append(backups, info)
	}

	return backups, nil
}

// DeleteBackup 删除备份
func (bm *BackupManager) DeleteBackup(backupID string) error {
	// 删除备份文件
	backupFile := filepath.Join(bm.backupDir, backupID+".tar.gz")
	if err := os.Remove(backupFile); err != nil && !os.IsNotExist(err) {
		return err
	}

	// 删除备份信息文件
	infoFile := filepath.Join(bm.backupDir, backupID+".json")
	if err := os.Remove(infoFile); err != nil && !os.IsNotExist(err) {
		return err
	}

	return nil
}
