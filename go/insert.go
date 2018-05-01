package main

import (
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	"encoding/json"
	"strings"
	"strconv"
	"log"
	"os"
	"time"
)

func insertMain() {
	logger := log.New(os.Stderr, "[INSERT] ", log.Ltime|log.Lshortfile)
	db, err := gorm.Open("mysql", MysqlUrl)
	defer db.Close()

	insertQueue := RedisQueue{"insert"}
	ocrQueue := RedisQueue{"ocr"}

	if err != nil {
		logger.Panic(err)
	}

	log.Println("Insert worker has started")

	for {
		msg := insertQueue.GetBytes()
		var chat ChatNew
		json.Unmarshal(msg, &chat)

		if chat.ChatId == 0 {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		for {
			if err := db.Create(&chat).Error; err != nil {
				logger.Printf("insert message error: %v", err)
			}
			break
		}

		client.HSet("insert_worker_status", "last", time.Now().Unix())
		client.HSet("insert_worker_status", "size", insertQueue.Size())

		if strings.HasPrefix(chat.Text, OcrHint) {
			ocrQueue.Put(strconv.Itoa(int(chat.ID)))
		}
	}
}
