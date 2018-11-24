package main

import (
	"encoding/json"
	"github.com/getsentry/raven-go"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

type OcrItem struct {
	Id    int `json:"id"`
	Tries int `json:"tries,omitempty"`
}

func MessageUniqueKey(chat ChatNew) string {
	return strconv.FormatInt(chat.ChatId, 10) + "-" + strconv.Itoa(chat.MessageId)
}

func insertMain() {
	logger := log.New(os.Stderr, "[INSERT] ", log.Ltime|log.Lshortfile)
	db, err := gorm.Open("mysql", MysqlUrl)
	defer db.Close()

	insertQueue := RedisQueue{"insert"}
	ocrQueue := RedisQueue{"ocr"}
	insertSet := RedisExpiringSet{"insert_set", 10}

	if err != nil {
		raven.CaptureErrorAndWait(err, map[string]string{"module": "insert", "func": "start"})
		logger.Panic(err)
	}

	log.Println("Insert worker has started")

	for {
		messages := insertQueue.BulkGetBytes(10)

		if len(messages) == 0 {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		tx := db.Begin()

		for _, msg := range messages {
			var chat ChatNew
			json.Unmarshal(msg, &chat)

			if chat.Flag == int16(ChatFlagNew) && insertSet.Contains(MessageUniqueKey(chat)) {
				// ignore if inserted
				continue
			}

			for {
				if err := tx.Create(&chat).Error; err != nil {
					logger.Printf("insert message error: %v", err)
					raven.CaptureErrorAndWait(err, map[string]string{"module": "insert", "func": "add"})
				}
				break
			}

			insertSet.Add(MessageUniqueKey(chat))
			client.HSet("insert_worker_status", "last", time.Now().Unix())
			client.HSet("insert_worker_status", "size", insertQueue.Size())

			if strings.HasPrefix(chat.Text, OcrHint) {
				item := OcrItem{Id: chat.ID, Tries: 0}
				b, _ := json.Marshal(&item)
				ocrQueue.PutBytes(b)
			}
		}

		if err := tx.Commit().Error; err != nil {
			tx.Rollback()
			for _, msg := range messages {
				insertQueue.PutBytes(msg)
			}

			logger.Printf("insert commit error: %v", err)
			raven.CaptureErrorAndWait(err, map[string]string{"module": "insert", "func": "commit"})
		}
	}
}
