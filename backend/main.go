package main

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

var (
	ctx = context.Background()
	rdb = redis.NewClient(&redis.Options{Addr: "localhost:6379"})
)

type StatusUpdate struct {
	ID     string      `json:"id"`
	Status string      `json:"status"`
	Result interface{} `json:"result"`
}

func main() {
    fmt.Println("Starting worker...")
	stream, group := "mystream", "mygroup"
	rdb.XGroupCreateMkStream(ctx, stream, group, "0")
	go worker("consumer-1", group, stream)
	select {}
}

func worker(consumer, group, stream string) {
	for {
		streams, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    group,
			Consumer: consumer,
			Streams:  []string{stream, ">"},
			Count:    1,
			Block:    0,
		}).Result()
		if err != nil {
			fmt.Println("Read error:", err)
			time.Sleep(time.Second)
			continue
		}

		for _, msg := range streams[0].Messages {
			id, _ := msg.Values["id"].(string)
			body, _ := msg.Values["body"].(string)
			fmt.Println("Processing:", id, body)

			time.Sleep(2 * time.Second)
			rdb.XAck(ctx, stream, group, msg.ID)
		}
	}
}
