package database

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	_ "modernc.org/sqlite"
)

// CreateOrUpdateUserEntityWithPathChange 在目标路径下创建用户实体记录。
//
// 旧实现会遍历父目录中的任意文件夹，只要数据库里存在同名记录就复用它，
// 完全不校验 user_id。结果是从第二个用户开始，所有用户都会关联到同一个
// （第一个）文件夹，媒体文件全部堆在一起。
//
// 现在路径迁移由 LocateUserEntity 通过文件夹内的 .tmd_user.json 标记来完成；
// 这里只负责在确认没有可复用记录时新建一条记录。
func CreateOrUpdateUserEntityWithPathChange(db *sqlx.DB, entity *UserEntity, rootPath string) (*UserEntity, error) {
	absPath, err := filepath.Abs(entity.ParentDir)
	if err != nil {
		return nil, err
	}
	entity.ParentDir = absPath

	err = CreateUserEntity(db, entity)
	if err != nil {
		return nil, err
	}
	return entity, nil
}

// CreateOrUpdateLstEntityWithPathChange 在目标路径下创建列表实体记录。
// 与用户实体一样，路径迁移由 LocateLstEntity 基于 .tmd_list.json 处理。
func CreateOrUpdateLstEntityWithPathChange(db *sqlx.DB, entity *LstEntity) (*LstEntity, error) {
	absPath, err := filepath.Abs(entity.ParentDir)
	if err != nil {
		return nil, err
	}
	entity.ParentDir = absPath

	if err := CreateLstEntity(db, entity); err != nil {
		return nil, err
	}
	return entity, nil
}

const schema = `
CREATE TABLE IF NOT EXISTS users (
	id INTEGER NOT NULL, 
	screen_name VARCHAR NOT NULL, 
	name VARCHAR NOT NULL, 
	protected BOOLEAN NOT NULL, 
	friends_count INTEGER NOT NULL, 
	PRIMARY KEY (id), 
	UNIQUE (screen_name)
);

CREATE TABLE IF NOT EXISTS user_previous_names (
	id INTEGER NOT NULL, 
	uid INTEGER NOT NULL, 
	screen_name VARCHAR NOT NULL, 
	name VARCHAR NOT NULL, 
	record_date DATE NOT NULL, 
	PRIMARY KEY (id), 
	FOREIGN KEY(uid) REFERENCES users (id)
);

CREATE TABLE IF NOT EXISTS lsts (
	id INTEGER NOT NULL, 
	name VARCHAR NOT NULL, 
	owner_uid INTEGER NOT NULL, 
	PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS lst_entities (
	id INTEGER NOT NULL, 
	lst_id INTEGER NOT NULL, 
	name VARCHAR NOT NULL, 
	parent_dir VARCHAR NOT NULL COLLATE NOCASE, 
	PRIMARY KEY (id), 
	UNIQUE (lst_id, parent_dir)
);

CREATE TABLE IF NOT EXISTS user_entities (
	id INTEGER NOT NULL, 
	user_id INTEGER NOT NULL, 
	name VARCHAR NOT NULL, 
	latest_release_time DATETIME, 
	parent_dir VARCHAR COLLATE NOCASE NOT NULL, 
	media_count INTEGER,
	PRIMARY KEY (id), 
	UNIQUE (user_id, parent_dir), 
	FOREIGN KEY(user_id) REFERENCES users (id)
);

CREATE TABLE IF NOT EXISTS user_links (
	id INTEGER NOT NULL,
	user_id INTEGER NOT NULL, 
	name VARCHAR NOT NULL, 
	parent_lst_entity_id INTEGER NOT NULL,
	PRIMARY KEY (id),
	UNIQUE (user_id, parent_lst_entity_id),
	FOREIGN KEY(user_id) REFERENCES users (id), 
	FOREIGN KEY(parent_lst_entity_id) REFERENCES lst_entities (id)
);

CREATE INDEX IF NOT EXISTS idx_user_links_user_id ON user_links (user_id);

CREATE TABLE IF NOT EXISTS media_files (
	id INTEGER NOT NULL,
	tweet_id INTEGER NOT NULL,
	url VARCHAR NOT NULL,
	file_name VARCHAR NOT NULL,
	user_entity_id INTEGER NOT NULL,
	PRIMARY KEY (id),
	UNIQUE (tweet_id, url)
);

CREATE INDEX IF NOT EXISTS idx_media_files_tweet_id ON media_files (tweet_id);
CREATE INDEX IF NOT EXISTS idx_media_files_entity_name ON media_files (user_entity_id, file_name);
`

func CreateTables(db *sqlx.DB) {
	db.MustExec(schema)
}

func CreateUser(db *sqlx.DB, usr *User) error {
	stmt := `INSERT INTO Users(id, screen_name, name, protected, friends_count) VALUES(:id, :screen_name, :name, :protected, :friends_count)`
	_, err := db.NamedExec(stmt, usr)
	return err
}

func DelUser(db *sqlx.DB, uid uint64) error {
	stmt := `DELETE FROM users WHERE id=?`
	_, err := db.Exec(stmt, uid)
	return err
}

func GetUserById(db *sqlx.DB, uid uint64) (*User, error) {
	stmt := `SELECT * FROM users WHERE id=?`
	result := &User{}
	err := db.Get(result, stmt, uid)
	if err == sql.ErrNoRows {
		result = nil
		err = nil
	}
	if err != nil {
		return nil, err
	}
	return result, nil
}

// GetUserByScreenName 按用户名查询用户，不存在时返回 nil, nil
func GetUserByScreenName(db *sqlx.DB, screenName string) (*User, error) {
	stmt := `SELECT * FROM users WHERE screen_name=? COLLATE NOCASE`
	result := &User{}
	err := db.Get(result, stmt, screenName)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return result, nil
}

func UpdateUser(db *sqlx.DB, usr *User) error {
	stmt := `UPDATE users SET screen_name=:screen_name, name=:name, protected=:protected, friends_count=:friends_count WHERE id=:id`
	_, err := db.NamedExec(stmt, usr)
	return err
}

// GetUserEntities 返回某个用户在数据库中的全部实体记录（只读，不做任何修正）
func GetUserEntities(db *sqlx.DB, uid uint64) ([]*UserEntity, error) {
	entities := make([]*UserEntity, 0)
	if err := db.Select(&entities, `SELECT * FROM user_entities WHERE user_id=?`, uid); err != nil {
		return nil, err
	}
	return entities, nil
}

func CreateUserEntity(db *sqlx.DB, entity *UserEntity) error {
	// 这里我们使用新的路径变更处理函数
	// 由于原始函数接口不支持传入rootPath参数，我们在这里简单包装
	// 注意：在main.go中调用时应该使用CreateOrUpdateUserEntityWithPathChange
	abs, err := filepath.Abs(entity.ParentDir)
	if err != nil {
		return err
	}
	entity.ParentDir = abs

	stmt := `INSERT INTO user_entities(user_id, name, parent_dir) VALUES(:user_id, :name, :parent_dir)`
	de, err := db.NamedExec(stmt, entity)
	if err != nil {
		return err
	}
	lastId, err := de.LastInsertId()
	if err != nil {
		return err
	}

	entity.Id.Scan(lastId)
	return nil
}

func DelUserEntity(db *sqlx.DB, id uint32) error {
	stmt := `DELETE FROM user_entities WHERE id=?`
	_, err := db.Exec(stmt, id)
	return err
}

// LocateUserEntity 定位某个用户在指定父目录下的实体记录。
//
// 查找顺序：
//  1. 用户文件夹中的 .tmd_user.json 标记（支持整个存储目录迁移）；
//  2. (user_id, parent_dir) 精确匹配；
//  3. 旧数据没有标记时，仅当该用户自己的记录名对应的文件夹确实存在才复用。
//
// 任何情况下都不会把其他用户的记录返回给当前用户。
// 若旧记录错误地指向了其他用户的文件夹，这里只返回记录本身，
// 由 syncUserAndEntity 修正名称后重新指向当前用户自己的文件夹。
func LocateUserEntity(db *sqlx.DB, uid uint64, parentDIr string) (*UserEntity, error) {
	absPath, err := filepath.Abs(parentDIr)
	if err != nil {
		return nil, err
	}

	// 该用户在数据库中的所有记录
	var entities []*UserEntity
	if err := db.Select(&entities, `SELECT * FROM user_entities WHERE user_id=?`, uid); err != nil {
		return nil, err
	}
	if len(entities) == 0 {
		return nil, nil
	}

	// 1. 通过文件夹标记定位（存储根目录迁移后仍然有效）
	if dirName := lookupUserDir(absPath, uid); dirName != "" {
		var target *UserEntity
		// 优先使用已经位于当前根目录下的记录，避免把其他根目录的记录
		// 搬过来时与它触发 UNIQUE(user_id, parent_dir) 冲突
		for _, e := range entities {
			if strings.EqualFold(e.ParentDir, absPath) {
				target = e
				break
			}
		}
		if target == nil {
			for _, e := range entities {
				if strings.EqualFold(e.Name, dirName) {
					target = e
					break
				}
			}
		}
		if target == nil && len(entities) == 1 {
			target = entities[0]
		}
		if target != nil {
			if !strings.EqualFold(target.ParentDir, absPath) || target.Name != dirName {
				if _, err := db.Exec(`UPDATE user_entities SET parent_dir=?, name=? WHERE id=?`,
					absPath, dirName, target.Id); err != nil {
					return nil, err
				}
				target.ParentDir = absPath
				target.Name = dirName
				fmt.Printf("路径匹配提示: 用户 %d 的下载记录已更新到新路径 '%s'\n", uid, filepath.Join(absPath, dirName))
			}
			return target, nil
		}
	}

	// 2. (user_id, parent_dir) 精确匹配。
	// 即使记录对应的文件夹属于其他用户，也返回该记录，交由调用方修正名称；
	// 直接返回 nil 会导致后续 INSERT 触发 UNIQUE(user_id, parent_dir) 冲突。
	result := &UserEntity{}
	err = db.Get(result, `SELECT * FROM user_entities WHERE user_id=? AND parent_dir=?`, uid, absPath)
	if err == nil {
		return result, nil
	}
	if err != sql.ErrNoRows {
		return nil, err
	}

	// 3. 兼容没有标记的旧数据：只有当该记录对应的文件夹确实存在时才复用
	for _, e := range entities {
		if e.Name == "" {
			continue
		}
		dir := filepath.Join(absPath, e.Name)
		info, err := os.Stat(dir)
		if err != nil || !info.IsDir() {
			continue
		}
		// 文件夹属于其他用户时绝不复用
		if marker, _ := ReadUserMarker(dir); marker != nil && marker.UserId != uid {
			continue
		}
		if _, err := db.Exec(`UPDATE user_entities SET parent_dir=? WHERE id=?`, absPath, e.Id); err != nil {
			return nil, err
		}
		e.ParentDir = absPath
		return e, nil
	}

	return nil, nil
}

func GetUserEntity(db *sqlx.DB, id int) (*UserEntity, error) {
	result := &UserEntity{}
	stmt := `SELECT * FROM user_entities WHERE id=?`
	err := db.Get(result, stmt, id)
	if err == sql.ErrNoRows {
		result = nil
		err = nil
	}
	if err != nil {
		return nil, err
	}
	return result, nil
}

func UpdateUserEntity(db *sqlx.DB, entity *UserEntity) error {
	// 只更新名称。latest_release_time / media_count 由下载完成后专门的函数更新，
	// 避免在下载开始前推进“最后下载时间”导致内容被永久跳过。
	stmt := `UPDATE user_entities SET name=? WHERE id=?`
	_, err := db.Exec(stmt, entity.Name, entity.Id.Int32)
	return err
}

func UpdateUserEntityMediCount(db *sqlx.DB, eid int, count int) error {
	stmt := `UPDATE user_entities SET media_count=? WHERE id=?`
	_, err := db.Exec(stmt, count, eid)
	return err
}

func UpdateUserEntityTweetStat(db *sqlx.DB, eid int, baseline time.Time, count int) error {
	stmt := `UPDATE user_entities SET latest_release_time=?, media_count=? WHERE id=?`
	_, err := db.Exec(stmt, baseline, count, eid)
	return err
}

func CreateLst(db *sqlx.DB, lst *Lst) error {
	stmt := `INSERT INTO lsts(id, name, owner_uid) VALUES(:id, :name, :owner_uid)`
	_, err := db.NamedExec(stmt, &lst)
	return err
}

func DelLst(db *sqlx.DB, lid uint64) error {
	stmt := `DELETE FROM lsts WHERE id=?`
	_, err := db.Exec(stmt, lid)
	return err
}

func GetLst(db *sqlx.DB, lid uint64) (*Lst, error) {
	stmt := `SELECT * FROM lsts WHERE id = ?`
	result := &Lst{}
	err := db.Get(result, stmt, lid)
	if err == sql.ErrNoRows {
		err = nil
		result = nil
	}
	if err != nil {
		return nil, err
	}
	return result, nil
}

func UpdateLst(db *sqlx.DB, lst *Lst) error {
	stmt := `UPDATE lsts SET name=? WHERE id=?`
	_, err := db.Exec(stmt, lst.Name, lst.Id)
	return err
}

func CreateLstEntity(db *sqlx.DB, entity *LstEntity) error {
	// 这里我们使用新的路径变更处理函数
	// 由于原始函数接口不支持复杂逻辑，我们在这里简单包装
	// 注意：在main.go中调用时应该使用CreateOrUpdateLstEntityWithPathChange
	abs, err := filepath.Abs(entity.ParentDir)
	if err != nil {
		return err
	}
	entity.ParentDir = abs

	stmt := `INSERT INTO lst_entities(lst_id, name, parent_dir) VALUES(:lst_id, :name, :parent_dir)`
	r, err := db.NamedExec(stmt, &entity)
	if err != nil {
		return err
	}
	id, err := r.LastInsertId()
	if err != nil {
		return err
	}
	entity.Id.Scan(id)
	return nil
}

func DelLstEntity(db *sqlx.DB, id int) error {
	stmt := `DELETE FROM lst_entities WHERE id=?`
	_, err := db.Exec(stmt, id)
	return err
}

func GetLstEntity(db *sqlx.DB, id int) (*LstEntity, error) {
	stmt := `SELECT * FROM lst_entities WHERE id=?`
	result := &LstEntity{}
	err := db.Get(result, stmt, id)
	if err == sql.ErrNoRows {
		err = nil
		result = nil
	}
	if err != nil {
		return nil, err
	}
	return result, nil
}

func LocateLstEntity(db *sqlx.DB, lid int64, parentDir string) (*LstEntity, error) {
	absPath, err := filepath.Abs(parentDir)
	if err != nil {
		return nil, err
	}

	var entities []*LstEntity
	if err := db.Select(&entities, `SELECT * FROM lst_entities WHERE lst_id=?`, lid); err != nil {
		return nil, err
	}
	if len(entities) == 0 {
		return nil, nil
	}

	// 1. 通过列表文件夹标记定位
	if dirName := lookupListDir(absPath, lid); dirName != "" {
		var target *LstEntity
		for _, e := range entities {
			if strings.EqualFold(e.ParentDir, absPath) {
				target = e
				break
			}
		}
		if target == nil {
			for _, e := range entities {
				if strings.EqualFold(e.Name, dirName) {
					target = e
					break
				}
			}
		}
		if target == nil && len(entities) == 1 {
			target = entities[0]
		}
		if target != nil {
			if !strings.EqualFold(target.ParentDir, absPath) || target.Name != dirName {
				if _, err := db.Exec(`UPDATE lst_entities SET parent_dir=?, name=? WHERE id=?`,
					absPath, dirName, target.Id); err != nil {
					return nil, err
				}
				target.ParentDir = absPath
				target.Name = dirName
				fmt.Printf("路径匹配提示: 列表 %d 的下载记录已更新到新路径 '%s'\n", lid, filepath.Join(absPath, dirName))
			}
			return target, nil
		}
	}

	// 2. 精确匹配路径
	result := &LstEntity{}
	err = db.Get(result, `SELECT * FROM lst_entities WHERE lst_id=? AND parent_dir=?`, lid, absPath)
	if err == nil {
		return result, nil
	}
	if err != sql.ErrNoRows {
		return nil, err
	}

	// 3. 兼容没有标记的旧数据：记录名对应的文件夹确实存在时才复用
	for _, e := range entities {
		if e.Name == "" {
			continue
		}
		dir := filepath.Join(absPath, e.Name)
		info, err := os.Stat(dir)
		if err != nil || !info.IsDir() {
			continue
		}
		if marker, _ := ReadListMarker(dir); marker != nil && marker.ListId != lid {
			continue
		}
		if _, err := db.Exec(`UPDATE lst_entities SET parent_dir=? WHERE id=?`, absPath, e.Id); err != nil {
			return nil, err
		}
		e.ParentDir = absPath
		return e, nil
	}

	return nil, nil
}
func UpdateLstEntity(db *sqlx.DB, entity *LstEntity) error {
	stmt := `UPDATE lst_entities SET name=? WHERE id=?`
	_, err := db.Exec(stmt, entity.Name, entity.Id.Int32)
	return err
}

func SetUserEntityLatestReleaseTime(db *sqlx.DB, id int, t time.Time) error {
	stmt := `UPDATE user_entities SET latest_release_time=? WHERE id=?`
	_, err := db.Exec(stmt, t, id)
	return err
}

func RecordUserPreviousName(db *sqlx.DB, uid uint64, name string, screenName string) error {
	stmt := `INSERT INTO user_previous_names(uid, screen_name, name, record_date) VALUES(?, ?, ?, ?)`
	_, err := db.Exec(stmt, uid, screenName, name, time.Now())
	return err
}

func CreateUserLink(db *sqlx.DB, lnk *UserLink) error {
	stmt := `INSERT INTO user_links(user_id, name, parent_lst_entity_id) VALUES(:user_id, :name, :parent_lst_entity_id)`
	res, err := db.NamedExec(stmt, lnk)
	if err != nil {
		return err
	}

	id, err := res.LastInsertId()
	if err != nil {
		return err
	}

	lnk.Id.Scan(id)
	return nil
}

func DelUserLink(db *sqlx.DB, id int32) error {
	stmt := `DELETE FROM user_links WHERE id = ?`
	_, err := db.Exec(stmt, id)
	return err
}

func GetUserLinks(db *sqlx.DB, uid uint64) ([]*UserLink, error) {
	stmt := `SELECT * FROM user_links WHERE user_id = ?`
	res := []*UserLink{}
	err := db.Select(&res, stmt, uid)
	return res, err
}

func GetUserLink(db *sqlx.DB, uid uint64, parentLstEntityId int32) (*UserLink, error) {
	stmt := `SELECT * FROM user_links WHERE user_id = ? AND parent_lst_entity_id = ?`
	res := &UserLink{}
	err := db.Get(res, stmt, uid, parentLstEntityId)
	if err == sql.ErrNoRows {
		err = nil
		res = nil
	}
	if err != nil {
		return nil, err
	}
	return res, nil
}

func UpdateUserLink(db *sqlx.DB, id int32, name string) error {
	stmt := `UPDATE user_links SET name = ? WHERE id = ?`
	_, err := db.Exec(stmt, name, id)
	return err
}

// GetMediaFile 查询某条推文的某个媒体地址是否已经下载
func GetMediaFile(db *sqlx.DB, tweetId uint64, url string) (*MediaFile, error) {
	result := &MediaFile{}
	err := db.Get(result, `SELECT * FROM media_files WHERE tweet_id=? AND url=?`, tweetId, url)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return result, nil
}

// GetMediaFileByName 查询同一用户目录下某个文件名是否已被其他媒体占用
func GetMediaFileByName(db *sqlx.DB, entityId int32, fileName string) (*MediaFile, error) {
	result := &MediaFile{}
	err := db.Get(result, `SELECT * FROM media_files WHERE user_entity_id=? AND file_name=?`, entityId, fileName)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return result, nil
}

// ListMediaFiles 返回某个用户实体下已记录的全部媒体文件
func ListMediaFiles(db *sqlx.DB, entityId int) ([]MediaFile, error) {
	files := make([]MediaFile, 0)
	if err := db.Select(&files, `SELECT * FROM media_files WHERE user_entity_id=?`, entityId); err != nil {
		return nil, err
	}
	return files, nil
}

// CreateMediaFile 记录（或更新）媒体文件映射。
// 下载前先写入记录可以“预定”文件名，避免并发下载同名文件相互覆盖。
func CreateMediaFile(db *sqlx.DB, media *MediaFile) error {
	stmt := `INSERT INTO media_files(tweet_id, url, file_name, user_entity_id)
VALUES(:tweet_id, :url, :file_name, :user_entity_id)
ON CONFLICT(tweet_id, url) DO UPDATE SET
file_name=excluded.file_name,
user_entity_id=excluded.user_entity_id`
	_, err := db.NamedExec(stmt, media)
	return err
}

// DeleteMediaFile 删除下载失败时预定的媒体记录，避免文件名被永久占用
func DeleteMediaFile(db *sqlx.DB, tweetId uint64, url string) error {
	_, err := db.Exec(`DELETE FROM media_files WHERE tweet_id=? AND url=?`, tweetId, url)
	return err
}
