package main

import (
	"github.com/go-redis/redis"
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
	b, _ := client.LPop(q.Name + QueuePrefix).Bytes()
	return b
}

func (q RedisQueue) Put(value string) {
	client.RPush(q.Name + QueuePrefix, value)
}

func (q RedisQueue) Size() int64 {
	return client.LLen(q.Name + QueuePrefix).Val()
}
