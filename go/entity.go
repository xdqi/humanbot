package main

import (
	_ "github.com/jinzhu/gorm/dialects/mysql"
	"github.com/jinzhu/gorm"
	"encoding/json"
	"time"
	"log"
	"os"
)

type EntityItem struct {
	EntityType string `json:"type"`
	User       User   `json:"user,omitempty"`
	Group      Group  `json:"group,omitempty"`
}

var logger = log.New(os.Stderr, "[ENTITY] ", log.Ltime | log.Lshortfile)

func updateUser(db *gorm.DB, newUser *User) {
	var user User
	if err := db.Where("uid = ?", newUser.UID).First(&user).Error; err != nil {
		// create new user
		for {
			if err := db.Create(&newUser).Error; err != nil {
				logger.Printf("create user error: %v", err)
			}
			break
		}
		return
	}
	// user exists
	same := newUser.FirstName == user.FirstName && newUser.LastName == user.LastName && newUser.Username == user.Username
	if same {
		// user not changed
		return
	}
	// user changed
	var firstHistory UsernameHistory
	if err := db.Where("uid = ?", newUser.UID).First(&firstHistory).Error; err != nil {
		// create first history
		firstHistory.UID = user.UID
		firstHistory.Username = user.Username
		firstHistory.FirstName = user.FirstName
		firstHistory.LastName = user.LastName
		firstHistory.LangCode = user.LangCode
		firstHistory.Date = 0
		for {
			if err := db.Create(&firstHistory).Error; err != nil {
				logger.Printf("create orig user history error: %v", err)
			}
			break
		}
	}
	// create new history
	history := UsernameHistory{
		UID:       newUser.UID,
		Username:  newUser.Username,
		FirstName: newUser.FirstName,
		LastName:  newUser.LastName,
		LangCode:  newUser.LangCode,
		Date:      int(time.Now().Unix()),
	}
	for {
		if err := db.Create(&history).Error; err != nil {
			logger.Printf("create new user history error: %v", err)
		}
		break
	}
	user.Username = newUser.Username
	user.FirstName = newUser.FirstName
	user.LastName = newUser.LastName
	user.LangCode = newUser.LangCode
	for {
		if err := db.Save(&user).Error; err != nil {
			logger.Printf("save modified user error: %v", err)
		}
		break
	}
}

func updateGroup(db *gorm.DB, newGroup *Group) {
	var group Group
	if err := db.Where("id = ?", newGroup.GID).First(&group).Error; err != nil {
		// create new group
		for {
			if err := db.Create(&newGroup).Error; err != nil {
				logger.Printf("create group error: %v", err)
			}
			break
		}
		return
	}
	// check master
	if group.Master.IsZero() {
		group.Master = newGroup.Master
		for {
			if err := db.Save(&group).Error; err != nil {
				logger.Printf("save group master error: %v", err)
			}
			break
		}
	}
	// group exists
	same := newGroup.Name == group.Name && newGroup.Link == group.Link
	if same {
		// group not changed
		return
	}
	// group changed
	var firstHistory GroupHistory
	if err := db.Where("gid = ?", newGroup.GID).First(&firstHistory).Error; err != nil {
		// create first history
		firstHistory.GID = group.GID
		firstHistory.Name = group.Name
		firstHistory.Link = group.Link
		firstHistory.Date = 0
		for {
			if err := db.Create(&firstHistory).Error; err != nil {
				logger.Printf("create orig group history error: %v", err)
			}
			break
		}
	}
	// create new history
	history := GroupHistory{
		GID:  newGroup.GID,
		Name: newGroup.Name,
		Link: newGroup.Link,
		Date: int(time.Now().Unix()),
	}
	for {
		if err := db.Create(&history).Error; err != nil {
			logger.Printf("create new group history error: %v", err)
		}
		break
	}
	// modify original object
	group.Name = newGroup.Name
	group.Link = newGroup.Link
	for {
		if err := db.Save(&group).Error; err != nil {
			logger.Printf("save modified group error: %v", err)
		}
		break
	}
}

func entityMain() {
	db, err := gorm.Open("mysql", MysqlUrl)
	defer db.Close()

	entityQueue := RedisQueue{"entity"}

	if err != nil {
		logger.Panic(err)
	}

	logger.Println("Entity worker has started")

	for {
		msg := entityQueue.GetBytes()

		if msg == nil {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		var entity EntityItem
		json.Unmarshal(msg, &entity)

		if entity.EntityType == "user" {
			updateUser(db, &entity.User)
		} else if entity.EntityType == "group" {
			updateGroup(db, &entity.Group)
		}

		client.HSet("entity_worker_status", "last", time.Now().Unix())
		client.HSet("entity_worker_status", "size", entityQueue.Size())
	}
}
