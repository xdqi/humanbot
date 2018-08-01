package main

import (
	"github.com/go-redis/redis"
	"github.com/getsentry/raven-go"
)

var client = redis.NewClient(RedisOptions)

const QueuePrefix = "_queue"

type RedisQueue struct {
	Name string
}

func (q RedisQueue) Get() string {
	return client.LPop(q.Name + QueuePrefix).String()
}

func (q RedisQueue) GetBytes() []byte {
	b, err := client.LPop(q.Name + QueuePrefix).Bytes()
	if err == redis.Nil {
		return nil
	} else if err != nil {
		raven.CaptureErrorAndWait(err, map[string]string{"module": "cache", "func": "get"})
	}
	return b
}

func (q RedisQueue) BulkGetBytes(count int) [][]byte {
	var result [][]byte
	for i := 0; i < count; i++ {
		if b := q.GetBytes(); b != nil {
			result = append(result, b)
		}
	}
	return result
}

func (q RedisQueue) Put(value string) {
	client.RPush(q.Name + QueuePrefix, value)
}

func (q RedisQueue) PutBytes(value []byte) {
	client.RPush(q.Name + QueuePrefix, value)
}

func (q RedisQueue) Size() int64 {
	return client.LLen(q.Name + QueuePrefix).Val()
}
