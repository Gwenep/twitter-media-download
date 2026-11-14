package database

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// 用户/列表文件夹中的标记文件
//
// 数据库记录中的路径是绝对路径，一旦迁移存储根目录（例如重装系统、换盘符、
// 移动文件夹），旧路径就会失效。仅在数据库里按路径匹配会导致：
//   - 认不出旧的下载记录，从而重复下载
//   - 或者误把新用户关联到别人的文件夹（所有内容下载到同一个文件夹）
//
// 在每个人的文件夹里放一个标记文件，以稳定的 user_id / list_id 作为身份，
// 这样无论根目录怎么变，都能把数据库记录与正确的文件夹对应起来。
const (
	UserMarkerName = ".tmd_user.json"
	ListMarkerName = ".tmd_list.json"
)

type UserMarker struct {
	UserId     uint64    `json:"user_id"`
	Name       string    `json:"name"`
	ScreenName string    `json:"screen_name"`
	UpdatedAt  time.Time `json:"updated_at"`
}

type ListMarker struct {
	ListId    int64     `json:"list_id"`
	Name      string    `json:"name"`
	UpdatedAt time.Time `json:"updated_at"`
}

// ReadUserMarker 读取文件夹中的用户标记，不存在或格式损坏时返回 nil, nil
func ReadUserMarker(dir string) (*UserMarker, error) {
	data, err := os.ReadFile(filepath.Join(dir, UserMarkerName))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	marker := &UserMarker{}
	if err := json.Unmarshal(data, marker); err != nil {
		// 标记文件损坏不应该阻止程序运行
		return nil, nil
	}
	if marker.UserId == 0 {
		return nil, nil
	}
	return marker, nil
}

// WriteUserMarker 写入/刷新用户标记
func WriteUserMarker(dir string, uid uint64, name string, screenName string) error {
	marker := UserMarker{
		UserId:     uid,
		Name:       name,
		ScreenName: screenName,
		UpdatedAt:  time.Now(),
	}
	data, err := json.MarshalIndent(marker, "", "    ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, UserMarkerName), data, 0644); err != nil {
		return err
	}
	registerUserDir(filepath.Dir(dir), uid, filepath.Base(dir))
	return nil
}

// ReadListMarker 读取文件夹中的列表标记
func ReadListMarker(dir string) (*ListMarker, error) {
	data, err := os.ReadFile(filepath.Join(dir, ListMarkerName))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	marker := &ListMarker{}
	if err := json.Unmarshal(data, marker); err != nil {
		return nil, nil
	}
	if marker.ListId == 0 {
		return nil, nil
	}
	return marker, nil
}

// WriteListMarker 写入/刷新列表标记
func WriteListMarker(dir string, lid int64, name string) error {
	marker := ListMarker{
		ListId:    lid,
		Name:      name,
		UpdatedAt: time.Now(),
	}
	data, err := json.MarshalIndent(marker, "", "    ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, ListMarkerName), data, 0644); err != nil {
		return err
	}
	registerListDir(filepath.Dir(dir), lid, filepath.Base(dir))
	return nil
}

// 每个父目录下 “身份 -> 文件夹名” 的索引，避免迁移根目录时对每个用户都扫描一遍磁盘
type dirMarkerIndex struct {
	mu     sync.Mutex
	loaded bool
	users  map[uint64]string
	lists  map[int64]string
}

var markerIndexes sync.Map // parent dir -> *dirMarkerIndex

func getMarkerIndex(parentDir string) *dirMarkerIndex {
	abs, err := filepath.Abs(parentDir)
	if err != nil {
		abs = parentDir
	}
	v, _ := markerIndexes.LoadOrStore(abs, &dirMarkerIndex{})
	return v.(*dirMarkerIndex)
}

func (idx *dirMarkerIndex) load(parentDir string) {
	if idx.loaded {
		return
	}
	idx.loaded = true
	idx.users = make(map[uint64]string)
	idx.lists = make(map[int64]string)

	entries, err := os.ReadDir(parentDir)
	if err != nil {
		return
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		dir := filepath.Join(parentDir, entry.Name())
		if m, _ := ReadUserMarker(dir); m != nil {
			idx.users[m.UserId] = entry.Name()
		}
		if m, _ := ReadListMarker(dir); m != nil {
			idx.lists[m.ListId] = entry.Name()
		}
	}
}

// lookupUserDir 返回 parentDir 下属于 uid 的文件夹名
func lookupUserDir(parentDir string, uid uint64) string {
	idx := getMarkerIndex(parentDir)
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.load(parentDir)
	return idx.users[uid]
}

func lookupListDir(parentDir string, lid int64) string {
	idx := getMarkerIndex(parentDir)
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.load(parentDir)
	return idx.lists[lid]
}

func registerUserDir(parentDir string, uid uint64, name string) {
	idx := getMarkerIndex(parentDir)
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if !idx.loaded {
		return // 尚未建立索引时会自行从磁盘读取
	}
	idx.users[uid] = name
}

func registerListDir(parentDir string, lid int64, name string) {
	idx := getMarkerIndex(parentDir)
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if !idx.loaded {
		return
	}
	idx.lists[lid] = name
}

// RegisterUserDir 在文件夹改名后刷新进程内的“身份 -> 文件夹名”索引。
// 索引未建立时无需处理，后续会直接从磁盘读取到正确的名称。
func RegisterUserDir(parentDir string, uid uint64, name string) {
	registerUserDir(parentDir, uid, name)
}

// RegisterListDir 在列表文件夹改名后刷新进程内索引
func RegisterListDir(parentDir string, lid int64, name string) {
	registerListDir(parentDir, lid, name)
}
