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


type MarkItem struct {
	ChatId     int64  `json:"chat_id"`
	MessageId  int    `json:"message_id"`
	Tries      int    `json:"tries,omitempty"`
}


func markMain() {
	logger := log.New(os.Stderr, "[MARKER] ", log.Ltime|log.Lshortfile)
	db, err := gorm.Open("mysql", MysqlUrl)
	defer db.Close()

	markQueue := RedisQueue{"mark"}

	if err != nil {
		raven.CaptureErrorAndWait(err, map[string]string{"module": "mark", "func": "start"})
		logger.Panic(err)
	}

	log.Println("Mark worker has started")

	for {
		messages := markQueue.BulkGetBytes(10)

		if len(messages) == 0 {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		tx := db.Begin()

		for _, msg := range messages {
			var item MarkItem
			json.Unmarshal(msg, &item)

			for {
				var count int
				db.Model(&ChatNew{}).Where(&ChatNew{ChatId: item.ChatId, MessageId: item.MessageId}).Count(&count)

				if count <= 0 {
					item.Tries += 1
					if item.Tries > 2 {
						break
					}
					if newMsg, err := json.Marshal(item); err != nil {
						logger.Printf("insert mark message back error: %v", err)
						raven.CaptureErrorAndWait(err, map[string]string{"module": "mark", "func": "insert"})
						break
					} else {
						markQueue.PutBytes(newMsg)
					}
				}

				err := db.Model(&ChatNew{}).Where(&ChatNew{ChatId: item.ChatId, MessageId: item.MessageId}).
					UpdateColumn("flag", gorm.Expr("flag | ?", ChatFlagDeleted)).Error

				if err != nil {
					logger.Printf("mark query error: %v", err)
					raven.CaptureErrorAndWait(err, map[string]string{"module": "mark", "func": "query"})
				}
				break
			}

			client.HSet("mark_worker_status", "last", time.Now().Unix())
			client.HSet("mark_worker_status", "size", markQueue.Size())
		}

		if err := tx.Commit().Error; err != nil {
			tx.Rollback()
			for _, msg := range messages {
				markQueue.PutBytes(msg)
			}

			logger.Printf("mark commit error: %v", err)
			raven.CaptureErrorAndWait(err, map[string]string{"module": "mark", "func": "commit"})
		}
	}
}
