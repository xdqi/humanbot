package main

import (
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	"encoding/json"
	"log"
	"os"
	"time"
	"github.com/getsentry/raven-go"
)


func inviteMain() {
	logger := log.New(os.Stderr, "[INVITE] ", log.Ltime|log.Lshortfile)
	db, err := gorm.Open("mysql", MysqlUrl)
	defer db.Close()

	inviteQueue := RedisQueue{"invite"}

	if err != nil {
		raven.CaptureErrorAndWait(err, map[string]string{"module": "invite", "func": "start"})
		logger.Panic(err)
	}

	log.Println("Invite worker has started")

	for {
		messages := inviteQueue.BulkGetBytes(10)

		if len(messages) == 0 {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		tx := db.Begin()

		for _, msg := range messages {
			var item GroupInvite
			json.Unmarshal(msg, &item)

			for {
				if err := tx.Create(&item).Error; err != nil {
					logger.Printf("add invite error: %v, %v", err, item.ID)
					raven.CaptureErrorAndWait(err, map[string]string{"module": "invite", "func": "add"})
					item.ID += 1
				}
			}

			client.HSet("invite_worker_status", "last", time.Now().Unix())
			client.HSet("invite_worker_status", "size", inviteQueue.Size())
		}

		if err := tx.Commit().Error; err != nil {
			tx.Rollback()
			for _, msg := range messages {
				var result []byte
				result, err = json.Marshal(msg)
				inviteQueue.PutBytes(result)
			}

			logger.Printf("invite commit error: %v", err)
			raven.CaptureErrorAndWait(err, map[string]string{"module": "invite", "func": "commit"})

		}
	}
}
