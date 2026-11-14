package downloading

import (
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/jmoiron/sqlx"
	log "github.com/sirupsen/logrus"
	"github.com/Gwenep/twitter-media-download/internal/database"
	"github.com/Gwenep/twitter-media-download/internal/twitter"
)

type TweetDumper struct {
	data  map[int][]*twitter.Tweet
	set   map[int]map[uint64]struct{}
	count int
}

func NewDumper() *TweetDumper {
	td := TweetDumper{}
	td.data = make(map[int][]*twitter.Tweet)
	td.set = make(map[int]map[uint64]struct{})
	return &td
}

func (td *TweetDumper) Push(eid int, tweet ...*twitter.Tweet) int {
	_, ok := td.data[eid]
	if !ok {
		td.data[eid] = make([]*twitter.Tweet, 0, len(tweet))
		td.set[eid] = make(map[uint64]struct{})
	}

	oldCount := td.count

	for _, tw := range tweet {
		_, exist := td.set[eid][tw.Id]
		if exist {
			continue
		}
		td.data[eid] = append(td.data[eid], tw)
		td.set[eid][tw.Id] = struct{}{}
		td.count++
	}
	return td.count - oldCount
}

func (td *TweetDumper) Load(path string) error {
	file, err := os.OpenFile(path, os.O_RDONLY, 0)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		return err
	}
	loaded := make(map[int][]*twitter.Tweet)
	err = json.Unmarshal(data, &loaded)
	if err != nil {
		return err
	}

	for k, v := range loaded {
		td.Push(k, v...)
	}
	return nil
}

func (td *TweetDumper) Dump(path string) error {
	data, err := json.MarshalIndent(td.data, "", "    ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0666)
}

func (td *TweetDumper) Clear() {
	td.data = make(map[int][]*twitter.Tweet)
	td.set = make(map[int]map[uint64]struct{})
	td.count = 0
}

func (td *TweetDumper) GetTotal(db *sqlx.DB) ([]*TweetInEntity, error) {
	results := make([]*TweetInEntity, 0, td.count)

	for k, v := range td.data {
		e, err := database.GetUserEntity(db, k)
		if err != nil {
			return nil, err
		}
		if e == nil {
			return nil, fmt.Errorf("entity %d is not exists", k)
		}

		for _, tw := range v {
			target := e

			// 旧版本曾把所有用户的失败推文都记在同一个实体（通常是第一个用户）下。
			// 直接按记录里的实体重试，会把媒体写进别人的文件夹。
			// 这里以推文作者为准：能找到作者的实体就改投，找不到就跳过，
			// 等下次正常拉取该用户时间线时再下载。
			if tw.Creator != nil && tw.Creator.Id != e.Uid {
				right, err := database.LocateUserEntity(db, tw.Creator.Id, e.ParentDir)
				if err != nil {
					return nil, err
				}
				if right == nil {
					log.WithFields(log.Fields{
						"tweet":   tw.Id,
						"author":  tw.Creator.Id,
						"entity":  e.Uid,
						"creator": tw.Creator.Title(),
					}).Warnln("failed tweet belongs to another user, skipped and will be fetched again")
					continue
				}
				target = right
			}

			ue := &UserEntity{db: db, record: target, created: true}
			results = append(results, &TweetInEntity{Tweet: tw, Entity: ue})
		}
	}
	return results, nil
}

func (td *TweetDumper) Count() int {
	return td.count
}
