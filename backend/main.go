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
	rdb *redis.Client
)

type StatusUpdate struct {
	ID     string      `json:"id"`
	Status string      `json:"status"`
	Result interface{} `json:"result"`
}

func main() {
	rdb = redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	streamName := "mystream"
	groupName := "mygroup"

	// Create the consumer group if it doesn't exist
	err := rdb.XGroupCreate(ctx, streamName, groupName, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		fmt.Println("Error creating group:", err)
	}

	// Start multiple worker goroutines
	for i := 0; i < 5; i++ {
		consumerName := fmt.Sprintf("consumer-%d", i)
		go worker(i, consumerName, groupName, streamName)
	}

	// Keep the main goroutine alive
	select {}
}

func worker(id int, consumer, group, stream string) {
	for {
		// Read new messages from the group
		streams, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    group,
			Consumer: consumer,
			Streams:  []string{stream, ">"},
			Count:    1,
			Block:    0,
		}).Result()

		if err != nil {
			fmt.Println("Error reading group:", err)
			time.Sleep(1 * time.Second)
			continue
		}

		for _, stream := range streams {
			for _, message := range stream.Messages {
				messageID, ok := message.Values["id"].(string)
				if !ok {
					fmt.Println("Invalid message ID format")
					continue
				}

				messageBody, _ := message.Values["body"].(string)
				fmt.Printf("Worker %d processing: %s\n", id, messageBody)

				// Update status to 'processing'
				updateStatus(messageID, "processing", nil)

				// Simulate processing time
				time.Sleep(2 * time.Second)

				// Process the message and get result
				result := fmt.Sprintf("Processed result for message %s", messageBody)

				// Update status to 'completed' with result
				updateStatus(messageID, "completed", result)

				// Acknowledge the message
				err := rdb.XAck(ctx, stream, group, message.ID).Err()
				if err != nil {
					fmt.Printf("Error acknowledging message: %v\n", err)
				} else {
					fmt.Printf("Worker %d acknowledged: %v\n", id, message.ID)
				}
			}
		}
	}
}

func updateStatus(id, status string, result interface{}) {
	statusUpdate := StatusUpdate{
		ID:     id,
		Status: status,
		Result: result,
	}

	jsonData, err := json.Marshal(statusUpdate)
	if err != nil {
		fmt.Println("Error marshaling status update:", err)
		return
	}

	resp, err := http.Post(
		"http://localhost:3000/update-status",
		"application/json",
		bytes.NewBuffer(jsonData),
	)
	
	if err != nil {
		fmt.Println("Error updating status:", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		fmt.Printf("Failed to update status, status code: %d\n", resp.StatusCode)
	}
}