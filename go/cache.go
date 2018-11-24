package main

import (
	"github.com/getsentry/raven-go"
	"github.com/go-redis/redis"
	"time"
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
		raven.CaptureErrorAndWait(err, map[string]string{"module": "cache", "func": "get_bytes"})
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
	client.RPush(q.Name+QueuePrefix, value)
}

func (q RedisQueue) PutBytes(value []byte) {
	client.RPush(q.Name+QueuePrefix, value)
}

func (q RedisQueue) Size() int64 {
	return client.LLen(q.Name + QueuePrefix).Val()
}

type RedisExpiringSet struct {
	Name   string
	Expire int64 // seconds
}

func (s RedisExpiringSet) Contains(item string) bool {
	saved, err := client.ZScore(s.Name, item).Result()
	now := time.Now().Unix()

	if err == redis.Nil {
		return false
	} else if err != nil {
		raven.CaptureErrorAndWait(err, map[string]string{"module": "cache", "func": "set_contains"})
	}
	if int64(saved)+s.Expire > now {
		client.ZAdd(s.Name, redis.Z{Score: float64(now), Member: item})
		return true
	}

	client.ZRem(s.Name, item)
	return false
}

func (s RedisExpiringSet) Add(item string) {
	client.ZAdd(s.Name, redis.Z{Score: float64(time.Now().Unix()), Member: item})
}

func (s RedisExpiringSet) Discard(item string) {
	client.ZRem(s.Name, item)
}

func (s RedisExpiringSet) Clear() {
	client.Del(s.Name)
}
