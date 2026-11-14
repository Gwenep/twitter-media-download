package downloading

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/Gwenep/twitter-media-download/internal/database"
	"github.com/Gwenep/twitter-media-download/internal/utils"
)

// 路径Plus
type SmartPath interface {
	Path() (string, error)
	Create(name string) error
	Rename(string) error
	Remove() error
	Name() string
	Id() int
	Recorded() bool
}

func syncPath(path SmartPath, expectedName string) error {
	if !path.Recorded() {
		return path.Create(expectedName)
	}

	if path.Name() != expectedName {
		return path.Rename(expectedName)
	}

	p, err := path.Path()
	if err != nil {
		return err
	}

	return os.MkdirAll(p, 0755)
}

// dirIsEmpty 判断目录是否为空，用于安全地复用/删除空目录
func dirIsEmpty(path string) (bool, error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return false, err
	}
	return len(entries) == 0, nil
}

type UserEntity struct {
	record  *database.UserEntity
	db      *sqlx.DB
	created bool
}

func NewUserEntity(db *sqlx.DB, uid uint64, parentDir string) (*UserEntity, error) {
	created := true
	record, err := database.LocateUserEntity(db, uid, parentDir)
	if err != nil {
		return nil, err
	}
	if record == nil {
		record = &database.UserEntity{}
		record.Uid = uid
		record.ParentDir = parentDir
		created = false
	}
	return &UserEntity{record: record, db: db, created: created}, nil
}

func (ue *UserEntity) Create(name string) error {
	ue.record.Name = name
	path, err := ue.Path()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(path, 0755); err != nil {
		return err
	}

	// 使用新的路径变更处理函数，支持路径变更时的记录关联
	updatedRecord, err := database.CreateOrUpdateUserEntityWithPathChange(ue.db, ue.record, filepath.Dir(path))
	if err != nil {
		return err
	}
	// 更新本地记录引用
	ue.record = updatedRecord
	ue.created = true
	return nil
}

func (ue *UserEntity) Remove() error {
	path, _ := ue.Path()

	if err := os.RemoveAll(path); err != nil {
		return err
	}
	if err := database.DelUserEntity(ue.db, uint32(ue.record.Id.Int32)); err != nil {
		return err
	}
	ue.created = false
	return nil
}

func (ue *UserEntity) Rename(title string) error {
	if !ue.created {
		return fmt.Errorf("user entity [%s:%d] was not created", ue.record.ParentDir, ue.record.Uid)
	}

	old, _ := ue.Path()
	newPath := filepath.Join(filepath.Dir(old), title)
	if filepath.Clean(old) == filepath.Clean(newPath) {
		return nil
	}

	// 目标目录已存在时不要继续：否则数据库会指向新目录，而旧目录里的文件
	// 会被遗留成“孤儿”，看起来就像内容丢失了。
	// 空目录可以直接删除后复用，避免用户手动留下的空文件夹阻塞同步。
	if exist, err := utils.PathExists(newPath); err != nil {
		return err
	} else if exist {
		empty, err := dirIsEmpty(newPath)
		if err != nil {
			return err
		}
		if !empty {
			return fmt.Errorf("target dir already exists: %s", newPath)
		}
		if err := os.Remove(newPath); err != nil {
			return err
		}
	}

	err := os.Rename(old, newPath)
	if os.IsNotExist(err) {
		err = os.MkdirAll(newPath, 0755)
	}
	if err != nil {
		return err
	}

	ue.record.Name = title
	if err := database.UpdateUserEntity(ue.db, ue.record); err != nil {
		return err
	}
	database.RegisterUserDir(filepath.Dir(newPath), ue.record.Uid, title)
	return nil
}

// RepairName 只修改数据库记录中的文件夹名，不移动磁盘目录。
//
// 旧版本可能把某个用户的记录错误地关联到别人的文件夹（同名匹配未校验 user_id）。
// 这种情况下绝不能调用 Rename 移动别人的文件夹，只需要让当前记录重新指向
// 属于自己的文件夹名，后续 syncPath 会创建/复用自己的目录。
func (ue *UserEntity) RepairName(title string) error {
	if !ue.created {
		return fmt.Errorf("user entity [%s:%d] was not created", ue.record.ParentDir, ue.record.Uid)
	}
	if ue.record.Name == title {
		return nil
	}
	ue.record.Name = title
	return database.UpdateUserEntity(ue.db, ue.record)
}

func (ue *UserEntity) Path() (string, error) {
	// 新建用户时记录里还没有文件夹名（Name 由 Create 填充），
	// 这里必须返回错误而不是让 database 层 panic，否则整个批次都会被中断。
	if ue.record == nil {
		return "", fmt.Errorf("user entity record was not loaded")
	}
	if ue.record.ParentDir == "" || ue.record.Name == "" {
		return "", fmt.Errorf("user entity [%s:%d] path is not ready", ue.record.ParentDir, ue.record.Uid)
	}
	return ue.record.Path(), nil
}

func (ue *UserEntity) Name() string {
	if ue.record.Name == "" {
		panic(fmt.Errorf("the name of user entity [%s:%d] was unset", ue.record.ParentDir, ue.record.Uid))
	}
	return ue.record.Name
}

func (ue *UserEntity) Id() int {
	if !ue.created {
		panic(fmt.Sprintf("user entity [%s:%d] was not created", ue.record.ParentDir, ue.record.Uid))
	}
	return int(ue.record.Id.Int32)
}

func (ue *UserEntity) LatestReleaseTime() time.Time {
	if !ue.created {
		panic(fmt.Sprintf("user entity [%s:%d] was not created", ue.record.ParentDir, ue.record.Uid))
	}
	return ue.record.LatestReleaseTime.Time
}

func (ue *UserEntity) SetLatestReleaseTime(t time.Time) error {
	if !ue.created {
		return fmt.Errorf("user entity [%s:%d] was not created", ue.record.ParentDir, ue.record.Uid)
	}
	err := database.SetUserEntityLatestReleaseTime(ue.db, int(ue.record.Id.Int32), t)
	if err == nil {
		ue.record.LatestReleaseTime.Scan(t)
	}
	return err
}

func (ue *UserEntity) Uid() uint64 {
	return ue.record.Uid
}

func (ue *UserEntity) Recorded() bool {
	return ue.created
}

type ListEntity struct {
	record  *database.LstEntity
	db      *sqlx.DB
	created bool
}

func NewListEntity(db *sqlx.DB, lid int64, parentDir string) (*ListEntity, error) {
	created := true
	record, err := database.LocateLstEntity(db, lid, parentDir)
	if err != nil {
		return nil, err
	}
	if record == nil {
		record = &database.LstEntity{}
		record.LstId = lid
		record.ParentDir = parentDir
		created = false
	}
	return &ListEntity{record: record, db: db, created: created}, nil
}

func (le *ListEntity) Create(name string) error {
	le.record.Name = name
	path, _ := le.Path()
	if err := os.MkdirAll(path, 0755); err != nil {
		return err
	}

	// 使用新的路径变更处理函数，支持路径变更时的记录关联
	updatedRecord, err := database.CreateOrUpdateLstEntityWithPathChange(le.db, le.record)
	if err != nil {
		return err
	}
	// 更新本地记录引用
	le.record = updatedRecord
	le.created = true
	return nil
}

func (le *ListEntity) Remove() error {
	if !le.created {
		return fmt.Errorf("list entity [%s:%d] was not created", le.record.ParentDir, le.record.LstId)
	}

	path, _ := le.Path()
	if err := os.RemoveAll(path); err != nil {
		return err
	}
	if err := database.DelLstEntity(le.db, int(le.record.Id.Int32)); err != nil {
		return err
	}
	le.created = false
	return nil
}

func (le *ListEntity) Rename(title string) error {
	if !le.created {
		return fmt.Errorf("list entity [%s:%d] was not created", le.record.ParentDir, le.record.LstId)
	}

	path, _ := le.Path()
	newPath := filepath.Join(filepath.Dir(path), title)
	if filepath.Clean(path) == filepath.Clean(newPath) {
		return nil
	}
	if exist, err := utils.PathExists(newPath); err != nil {
		return err
	} else if exist {
		empty, err := dirIsEmpty(newPath)
		if err != nil {
			return err
		}
		if !empty {
			return fmt.Errorf("target dir already exists: %s", newPath)
		}
		if err := os.Remove(newPath); err != nil {
			return err
		}
	}
	err := os.Rename(path, newPath)
	if os.IsNotExist(err) {
		err = os.Mkdir(newPath, 0755)
	}
	if err != nil {
		return err
	}

	le.record.Name = title
	if err := database.UpdateLstEntity(le.db, le.record); err != nil {
		return err
	}
	database.RegisterListDir(filepath.Dir(newPath), le.record.LstId, title)
	return nil
}

// RepairName 只修改数据库记录中的文件夹名，不移动磁盘目录。
// 用于旧版本错误关联到其他列表文件夹的情况。
func (le *ListEntity) RepairName(title string) error {
	if !le.created {
		return fmt.Errorf("list entity [%s:%d] was not created", le.record.ParentDir, le.record.LstId)
	}
	if le.record.Name == title {
		return nil
	}
	le.record.Name = title
	return database.UpdateLstEntity(le.db, le.record)
}

func (le *ListEntity) Path() (string, error) {
	// 同 UserEntity.Path：新建列表时 Name 尚未填充，不能直接 panic。
	if le.record == nil {
		return "", fmt.Errorf("list entity record was not loaded")
	}
	if le.record.ParentDir == "" || le.record.Name == "" {
		return "", fmt.Errorf("list entity [%s:%d] path is not ready", le.record.ParentDir, le.record.LstId)
	}
	return le.record.Path(), nil
}

func (le ListEntity) Name() string {
	if le.record.Name == "" {
		panic(fmt.Sprintf("the name of list entity [%s:%d] was unset", le.record.ParentDir, le.record.LstId))
	}

	return le.record.Name
}

func (le *ListEntity) Id() int {
	if !le.created {
		panic(fmt.Sprintf("list entity [%s:%d] was not created", le.record.ParentDir, le.record.LstId))
	}

	return int(le.record.Id.Int32)
}

func (le *ListEntity) Recorded() bool {
	return le.created
}

func updateUserLink(lnk *database.UserLink, db *sqlx.DB, path string) error {
	name := filepath.Base(path)

	linkpath, err := lnk.Path(db)
	if err != nil {
		return err
	}
	path, err = filepath.Abs(path)
	if err != nil {
		return err
	}

	if lnk.Name == name {
		// 用户未改名，但仍应确保链接存在且指向正确位置。
		// 存储目录迁移后，旧链接仍指向旧根目录，需要重建。
		if target, err := os.Readlink(linkpath); err == nil {
			if strings.EqualFold(filepath.Clean(target), filepath.Clean(path)) {
				return nil
			}
			if err := os.Remove(linkpath); err != nil {
				return err
			}
		}
		err = os.Symlink(path, linkpath)
		if os.IsExist(err) {
			err = nil
		}
		return err
	}

	newlinkpath := filepath.Join(filepath.Dir(linkpath), name)

	if err = os.RemoveAll(linkpath); err != nil {
		return err
	}
	if err = os.Symlink(path, newlinkpath); err != nil && !os.IsExist(err) {
		return err
	}

	if err = database.UpdateUserLink(db, lnk.Id.Int32, name); err != nil {
		return err
	}

	lnk.Name = name
	return nil
}
