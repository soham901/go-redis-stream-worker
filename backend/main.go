package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
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
	Result any `json:"result"`
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
			fmt.Println("Processing:", body)

			updateStatus(id, "processing", nil)
			time.Sleep(2 * time.Second)
			updateStatus(id, "completed", "Processed: "+body)
			rdb.XAck(ctx, stream, group, msg.ID)
		}
	}
}

func updateStatus(id, status string, result interface{}) {
	data, _ := json.Marshal(StatusUpdate{ID: id, Status: status, Result: result})
	resp, err := http.Post("http://localhost:3000/update-status", "application/json", bytes.NewBuffer(data))
	if err != nil || resp.StatusCode != http.StatusOK {
		fmt.Println("Status update failed")
	}
	if resp != nil {
		resp.Body.Close()
	}
}
