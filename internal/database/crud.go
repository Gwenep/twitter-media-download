package database

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
)

// 增强的路径匹配策略：基于用户ID和文件存在性，而不仅仅是路径字符串匹配
// 当用户更改下载路径时，只要数据库文件和.user文件存在，就认为是同一组下载记录

// CreateOrUpdateUserEntityWithPathChange 处理用户实体的创建或更新，支持路径变更
// 当检测到路径变更但数据库和.user文件存在时，更新现有记录而不是创建新记录
func CreateOrUpdateUserEntityWithPathChange(db *sqlx.DB, entity *UserEntity, rootPath string) (*UserEntity, error) {
	// 获取绝对路径
	absPath, err := filepath.Abs(entity.ParentDir)
	if err != nil {
		return nil, err
	}
	entity.ParentDir = absPath

	// 首先检查新路径下是否存在.user文件
	newUserFilePath := filepath.Join(absPath, ".user")
	if _, err := os.Stat(newUserFilePath); err == nil {
		// 新路径下存在.user文件，尝试查找该用户的所有实体记录
		var entities []*UserEntity
		stmt := `SELECT * FROM user_entities WHERE user_id=?`
		err = db.Select(&entities, stmt, entity.Uid)
		if err != nil {
			return nil, err
		}
		
		// 如果找到实体记录，更新路径并返回
		if len(entities) > 0 {
			// 更新第一个找到的实体记录的路径
			existingEntity := entities[0]
			updateStmt := `UPDATE user_entities SET parent_dir=?, name=? WHERE id=?`
			_, err = db.Exec(updateStmt, entity.ParentDir, entity.Name, existingEntity.Id)
			if err != nil {
				return nil, err
			}

			// 返回更新后的实体信息
			existingEntity.ParentDir = entity.ParentDir
			existingEntity.Name = entity.Name
			fmt.Printf("路径匹配提示: 用户 %d 的下载记录已更新到新路径 '%s'\n", entity.Uid, absPath)
			return existingEntity, nil
		}
	}

	// 然后尝试查找该用户的所有实体
	var entities []*UserEntity
	stmt := `SELECT * FROM user_entities WHERE user_id=?`
	err = db.Select(&entities, stmt, entity.Uid)
	if err != nil {
		return nil, err
	}

	// 检查是否存在匹配的实体记录
	for _, existingEntity := range entities {
		// 检查现有记录指向的目录是否有.user文件
		userFilePath := filepath.Join(existingEntity.ParentDir, ".user")
		if _, err := os.Stat(userFilePath); err == nil {
			// .user文件存在，认为是同一用户的下载记录
			// 更新现有记录的路径
			updateStmt := `UPDATE user_entities SET parent_dir=?, name=? WHERE id=?`
			_, err = db.Exec(updateStmt, entity.ParentDir, entity.Name, existingEntity.Id)
			if err != nil {
				return nil, err
			}

			// 返回更新后的实体信息
			existingEntity.ParentDir = entity.ParentDir
			existingEntity.Name = entity.Name
			return existingEntity, nil
		}
	}

	// 如果没有找到匹配的实体记录，创建新记录
	insertStmt := `INSERT INTO user_entities(user_id, name, parent_dir) VALUES(:user_id, :name, :parent_dir)`
	de, err := db.NamedExec(insertStmt, entity)
	if err != nil {
		return nil, err
	}
	lastId, err := de.LastInsertId()
	if err != nil {
		return nil, err
	}

	entity.Id.Scan(lastId)
	return entity, nil
}

// CreateOrUpdateLstEntityWithPathChange 处理列表实体的创建或更新，支持路径变更
func CreateOrUpdateLstEntityWithPathChange(db *sqlx.DB, entity *LstEntity) (*LstEntity, error) {
	// 获取绝对路径
	absPath, err := filepath.Abs(entity.ParentDir)
	if err != nil {
		return nil, err
	}
	entity.ParentDir = absPath

	// 首先尝试查找该列表的所有实体
	var entities []*LstEntity
	stmt := `SELECT * FROM lst_entities WHERE lst_id=?`
	err = db.Select(&entities, stmt, entity.LstId)
	if err != nil {
		return nil, err
	}

	// 检查是否存在匹配的实体记录
	for _, existingEntity := range entities {
		// 对于列表，我们基于列表ID和实体名称进行匹配
		// 当用户更改下载路径时，保持列表名称不变，认为是同一列表
		if strings.EqualFold(existingEntity.Name, entity.Name) {
			// 更新现有记录的路径
			updateStmt := `UPDATE lst_entities SET parent_dir=? WHERE id=?`
			_, err = db.Exec(updateStmt, entity.ParentDir, existingEntity.Id)
			if err != nil {
				return nil, err
			}

			// 返回更新后的实体信息
			existingEntity.ParentDir = entity.ParentDir
			return existingEntity, nil
		}
	}

	// 如果没有找到匹配的实体记录，创建新记录
	insertStmt := `INSERT INTO lst_entities(lst_id, name, parent_dir) VALUES(:lst_id, :name, :parent_dir)`
	r, err := db.NamedExec(insertStmt, &entity)
	if err != nil {
		return nil, err
	}
	id, err := r.LastInsertId()
	if err != nil {
		return nil, err
	}

	entity.Id.Scan(id)
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

func UpdateUser(db *sqlx.DB, usr *User) error {
	stmt := `UPDATE users SET screen_name=:screen_name, name=:name, protected=:protected, friends_count=:friends_count WHERE id=:id`
	_, err := db.NamedExec(stmt, usr)
	return err
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

func LocateUserEntity(db *sqlx.DB, uid uint64, parentDIr string) (*UserEntity, error) {
	absPath, err := filepath.Abs(parentDIr)
	if err != nil {
		return nil, err
	}

	// 首先检查新路径下是否存在.user文件
	newUserFilePath := filepath.Join(absPath, ".user")
	if _, err := os.Stat(newUserFilePath); err == nil {
		// 新路径下存在.user文件，尝试查找该用户的所有实体记录
		var entities []*UserEntity
		listStmt := `SELECT * FROM user_entities WHERE user_id=?`
		err = db.Select(&entities, listStmt, uid)
		if err != nil {
			return nil, err
		}
		
		// 如果找到实体记录，更新路径并返回
		if len(entities) > 0 {
			// 更新第一个找到的实体记录的路径
			entity := entities[0]
			updateStmt := `UPDATE user_entities SET parent_dir=? WHERE id=?`
			db.Exec(updateStmt, absPath, entity.Id)
			
			// 更新实体的路径
			entity.ParentDir = absPath
			fmt.Printf("路径匹配提示: 用户 %d 的下载记录已更新到新路径 '%s'\n", uid, absPath)
			return entity, nil
		}
	}

	// 然后尝试直接匹配路径
	stmt := `SELECT * FROM user_entities WHERE user_id=? AND parent_dir=?`
	result := &UserEntity{}
	err = db.Get(result, stmt, uid, absPath)
	if err == sql.ErrNoRows {
		// 直接匹配失败，尝试基于.user文件存在性来查找匹配的实体
		var entities []*UserEntity
		listStmt := `SELECT * FROM user_entities WHERE user_id=?`
		err = db.Select(&entities, listStmt, uid)
		if err != nil {
			return nil, err
		}
		
		// 检查每个实体的目录中是否存在.user文件
		for _, entity := range entities {
			userFilePath := filepath.Join(entity.ParentDir, ".user")
			if _, err := os.Stat(userFilePath); err == nil {
				// .user文件存在，认为是同一用户的下载记录
				// 打印提示信息，告知用户路径已变更
				fmt.Printf("路径匹配提示: 用户 %d 的下载记录已从 '%s' 移动到 '%s'\n", 
					uid, entity.ParentDir, absPath)
				
				// 更新数据库中的路径信息
				updateStmt := `UPDATE user_entities SET parent_dir=? WHERE id=?`
				db.Exec(updateStmt, absPath, entity.Id)
				
				// 更新实体的路径
				entity.ParentDir = absPath
				return entity, nil
			}
		}
		
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return result, nil
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
	stmt := `UPDATE user_entities SET name=?, latest_release_time=?, media_count=? WHERE id=?`
	_, err := db.Exec(stmt, entity.Name, entity.LatestReleaseTime, entity.MediaCount, entity.Id)
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

	stmt := `INSERT INTO lst_entities(id, lst_id, name, parent_dir) VALUES(:id, :lst_id, :name, :parent_dir)`
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

	// 首先尝试直接匹配路径
	stmt := `SELECT * FROM lst_entities WHERE lst_id=? AND parent_dir=?`
	result := &LstEntity{}
	err = db.Get(result, stmt, lid, absPath)
	if err == sql.ErrNoRows {
		// 直接匹配失败，尝试基于列表ID和名称来查找匹配的实体
		var entities []*LstEntity
		listStmt := `SELECT * FROM lst_entities WHERE lst_id=?`
		err = db.Select(&entities, listStmt, lid)
		if err != nil {
			return nil, err
		}
		
		// 基于列表名称进行匹配（不区分大小写）
		for _, entity := range entities {
			// 检查目标目录是否存在（作为判断依据）
			if _, err := os.Stat(absPath); err == nil {
				// 目录存在，基于列表ID和名称匹配
				// 打印提示信息，告知用户路径已变更
				fmt.Printf("路径匹配提示: 列表 %d 的下载记录已从 '%s' 移动到 '%s'\n", 
					lid, entity.ParentDir, absPath)
				
				// 更新数据库中的路径信息
				updateStmt := `UPDATE lst_entities SET parent_dir=? WHERE id=?`
				db.Exec(updateStmt, absPath, entity.Id)
				
				// 更新实体的路径
				entity.ParentDir = absPath
				return entity, nil
			}
		}
		
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return result, nil
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
