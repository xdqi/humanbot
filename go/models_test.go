package main

import (
	"testing"
	"encoding/json"
	"gopkg.in/guregu/null.v3"
	"reflect"
	"fmt"
)

func TestNormalChatUnmarshal(t *testing.T) {
	sample := []byte("{\"chat_id\": -1001246822000, \"message_id\": 128760, \"user_id\": 565713943, \"text\": \"\\u522b\\u4eba\\u6050\\u60e7\\u65f6\\u5019\\u6211\\u8d2a\\u5a6a\\u3002\\u53ef\\u662f\\u600e\\u4e48\\u8d2a\\u5a6a\\uff1f\\u6211\\u4e5f\\u5f88\\u6050\\u60e7\", \"date\": 1525185960, \"flag\": 0}")
	sampleChat := ChatNew{
		ChatId:    -1001246822000,
		MessageId: 128760,
		UserId:    null.IntFrom(565713943),
		Text:      "\u522b\u4eba\u6050\u60e7\u65f6\u5019\u6211\u8d2a\u5a6a\u3002\u53ef\u662f\u600e\u4e48\u8d2a\u5a6a\uff1f\u6211\u4e5f\u5f88\u6050\u60e7",
		Date:      1525185960,
		Flag:      0,
	}
	var chat ChatNew
	json.Unmarshal(sample, &chat)
	if reflect.DeepEqual(chat, sampleChat) {
		t.Log("same")
	} else {
		t.Error("different")
		fmt.Printf("chat: %+v\n", chat)
		fmt.Printf("sample: %+v\n", sampleChat)
	}
}


func TestNullChatUnmarshal(t *testing.T) {
	sample := []byte("{\"chat_id\": -1001246822000, \"message_id\": 128760, \"user_id\": null, \"text\": \"\\u522b\\u4eba\\u6050\\u60e7\\u65f6\\u5019\\u6211\\u8d2a\\u5a6a\\u3002\\u53ef\\u662f\\u600e\\u4e48\\u8d2a\\u5a6a\\uff1f\\u6211\\u4e5f\\u5f88\\u6050\\u60e7\", \"date\": 1525185960, \"flag\": 0}")
	sampleChat := ChatNew{
		ChatId:    -1001246822000,
		MessageId: 128760,
		UserId:    null.IntFromPtr(nil),
		Text:      "\u522b\u4eba\u6050\u60e7\u65f6\u5019\u6211\u8d2a\u5a6a\u3002\u53ef\u662f\u600e\u4e48\u8d2a\u5a6a\uff1f\u6211\u4e5f\u5f88\u6050\u60e7",
		Date:      1525185960,
		Flag:      0,
	}
	var chat ChatNew
	json.Unmarshal(sample, &chat)
	if reflect.DeepEqual(chat, sampleChat) {
		t.Log("same")
	} else {
		t.Error("different")
		fmt.Printf("chat: %+v\n", chat)
		fmt.Printf("sample: %+v\n", sampleChat)
	}
}
