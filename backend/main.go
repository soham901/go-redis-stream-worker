package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/joho/godotenv"
)

// Config holds all application configuration
type Config struct {
	RedisHost     string
	RedisPort     string
	ApiURL        string
	WorkerCount   int
	StreamName    string
	GroupName     string
	ProcessingTime time.Duration
}

// StatusUpdate represents a message status update
type StatusUpdate struct {
	ID     string `json:"id"`
	Status string `json:"status"`
	Result any    `json:"result"`
}

// Worker represents a message processing worker
type Worker struct {
	id         int
	consumer   string
	group      string
	stream     string
	redisClient *redis.Client
	config     *Config
	logger     *log.Logger
}

func main() {
	// Setup logger
	logger := log.New(os.Stdout, "[WORKER] ", log.LstdFlags|log.Lshortfile)
	
	// Load configuration
	config, err := loadConfig()
	if err != nil {
		logger.Fatalf("Failed to load configuration: %v", err)
	}
	
	logger.Printf("Starting worker with configuration: %+v", config)
	
	// Create Redis client
	redisClient := redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("%s:%s", config.RedisHost, config.RedisPort),
	})
	
	// Ping Redis to ensure connection
	if _, err := redisClient.Ping(context.Background()).Result(); err != nil {
		logger.Fatalf("Failed to connect to Redis: %v", err)
	}
	
	// Create the consumer group if it doesn't exist
	err = createConsumerGroup(redisClient, config)
	if err != nil {
		logger.Fatalf("Failed to create consumer group: %v", err)
	}
	
	// Setup graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	// Handle termination signals
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	
	// WaitGroup to track all workers
	var wg sync.WaitGroup
	
	// Start workers
	for i := 0; i < config.WorkerCount; i++ {
		wg.Add(1)
		worker := &Worker{
			id:          i,
			consumer:    fmt.Sprintf("consumer-%d", i),
			group:       config.GroupName,
			stream:      config.StreamName,
			redisClient: redisClient,
			config:      config,
			logger:      log.New(os.Stdout, fmt.Sprintf("[WORKER-%d] ", i), log.LstdFlags),
		}
		
		go func(w *Worker) {
			defer wg.Done()
			w.run(ctx)
		}(worker)
	}
	
	// Wait for termination signal
	<-signalChan
	logger.Println("Received termination signal, shutting down...")
	cancel()
	
	// Wait for all workers to finish with a timeout
	waitCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(waitCh)
	}()
	
	select {
	case <-waitCh:
		logger.Println("All workers shut down gracefully")
	case <-time.After(10 * time.Second):
		logger.Println("Timed out waiting for workers to shut down")
	}
	
	// Close Redis connection
	if err := redisClient.Close(); err != nil {
		logger.Printf("Error closing Redis connection: %v", err)
	}
}

// loadConfig loads application configuration from environment
func loadConfig() (*Config, error) {
	// Load .env file if it exists
	if err := godotenv.Load(".env"); err != nil {
		// Just log and continue, this is not fatal as env vars might be set another way
		log.Printf("Warning: Error loading .env file: %v", err)
	}
	
	// Get worker count with fallback to default
	workerCount := 5
	if wcStr := os.Getenv("WORKER_COUNT"); wcStr != "" {
		wc, err := strconv.Atoi(wcStr)
		if err != nil {
			return nil, fmt.Errorf("invalid WORKER_COUNT: %w", err)
		}
		workerCount = wc
	}
	
	// Get processing time with fallback to default
	processingTime := 2 * time.Second
	if ptStr := os.Getenv("PROCESSING_TIME"); ptStr != "" {
		pt, err := strconv.Atoi(ptStr)
		if err != nil {
			return nil, fmt.Errorf("invalid PROCESSING_TIME: %w", err)
		}
		processingTime = time.Duration(pt) * time.Millisecond
	}
	
	// Set defaults for optional values
	streamName := os.Getenv("STREAM_NAME")
	if streamName == "" {
		streamName = "mystream"
	}
	
	groupName := os.Getenv("GROUP_NAME")
	if groupName == "" {
		groupName = "mygroup"
	}
	
	redisHost := os.Getenv("REDIS_HOST")
	if redisHost == "" {
		redisHost = "localhost"
	}
	
	redisPort := os.Getenv("REDIS_PORT")
	if redisPort == "" {
		redisPort = "6379"
	}
	
	apiURL := os.Getenv("API_URL")
	if apiURL == "" {
		apiURL = "http://localhost:3000"
	}
	
	return &Config{
		RedisHost:     redisHost,
		RedisPort:     redisPort,
		ApiURL:        apiURL,
		WorkerCount:   workerCount,
		StreamName:    streamName,
		GroupName:     groupName,
		ProcessingTime: processingTime,
	}, nil
}

// createConsumerGroup creates a Redis stream consumer group if it doesn't exist
func createConsumerGroup(redisClient *redis.Client, config *Config) error {
	err := redisClient.XGroupCreate(context.Background(), config.StreamName, config.GroupName, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return err
	}
	return nil
}

// run starts the worker's processing loop
func (w *Worker) run(ctx context.Context) {
	w.logger.Printf("Starting worker %d", w.id)
	
	for {
		select {
		case <-ctx.Done():
			w.logger.Printf("Worker %d shutting down", w.id)
			return
		default:
			// Continue processing
		}
		
		// Read new messages from the group
		streams, err := w.redisClient.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    w.group,
			Consumer: w.consumer,
			Streams:  []string{w.stream, ">"},
			Count:    1, // Process one message at a time for better error handling
			Block:    5 * time.Second, // Use a timeout to check for context cancellation
		}).Result()
		
		if err != nil {
			if err == context.Canceled {
				return
			}
			if err != redis.Nil {
				w.logger.Printf("Error reading group: %v", err)
			}
			time.Sleep(1 * time.Second)
			continue
		}
		
		if len(streams) == 0 {
			continue
		}
		
		for _, stream := range streams {
			for _, message := range stream.Messages {
				w.processMessage(message)
			}
		}
	}
}

// processMessage handles a single message from the stream
func (w *Worker) processMessage(message redis.XMessage) {
	messageID, ok := message.Values["id"].(string)
	if !ok {
		w.logger.Println("Invalid message ID format")
		// Acknowledge the message to prevent reprocessing
		w.acknowledgeMessage(message.ID)
		return
	}
	
	messageBody, _ := message.Values["body"].(string)
	w.logger.Printf("Processing message: %s", messageBody)
	
	// Update status to 'processing'
	if err := w.updateStatus(messageID, "processing", nil); err != nil {
		w.logger.Printf("Failed to update status to processing: %v", err)
		// Continue processing despite update failure
	}
	
	// Simulate processing time
	time.Sleep(w.config.ProcessingTime)
	
	// Process the message and get result
	result := fmt.Sprintf("Processed result for message %s by worker %d", messageBody, w.id)
	
	// Update status to 'completed' with result
	if err := w.updateStatus(messageID, "completed", result); err != nil {
		w.logger.Printf("Failed to update status to completed: %v", err)
	}
	
	// Acknowledge the message
	w.acknowledgeMessage(message.ID)
}

// updateStatus sends a status update to the API
func (w *Worker) updateStatus(id, status string, result interface{}) error {
	statusUpdate := StatusUpdate{
		ID:     id,
		Status: status,
		Result: result,
	}
	
	jsonData, err := json.Marshal(statusUpdate)
	if err != nil {
		return fmt.Errorf("error marshaling status update: %w", err)
	}
	
	// Create a context with timeout for the HTTP request
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	// Create a new request with the context
	req, err := http.NewRequestWithContext(ctx, "POST", 
		w.config.ApiURL+"/update-status", bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("error creating request: %w", err)
	}
	
	req.Header.Set("Content-Type", "application/json")
	
	// Use a client with reasonable timeouts
	client := &http.Client{
		Timeout: 5 * time.Second,
	}
	
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("error updating status: %w", err)
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to update status, status code: %d", resp.StatusCode)
	}
	
	return nil
}

// acknowledgeMessage acknowledges a message in the stream
func (w *Worker) acknowledgeMessage(messageID string) {
	err := w.redisClient.XAck(context.Background(), w.stream, w.group, messageID).Err()
	if err != nil {
		w.logger.Printf("Error acknowledging message: %v", err)
	} else {
		w.logger.Printf("Acknowledged message: %v", messageID)
	}
}