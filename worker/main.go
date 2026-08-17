package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/go-redis/redis/v8"
	_ "github.com/lib/pq"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/segmentio/kafka-go"
	"github.com/sde-project/distributed-file-processing/pkg/models"
)

var (
	minioClient *minio.Client
	redisClient *redis.Client
	db          *sql.DB
	ctx         = context.Background()
)

func init() {
	var err error
	// MinIO
	minioClient, err = minio.New("localhost:9000", &minio.Options{
		Creds:  credentials.NewStaticV4("minioadmin", "minioadmin", ""),
		Secure: false,
	})
	if err != nil {
		log.Fatalln(err)
	}

	// Redis
	redisClient = redis.NewClient(&redis.Options{Addr: "localhost:6379"})

	// Postgres
	connStr := "user=sde_user password=sde_password dbname=sales_db sslmode=disable host=localhost"
	db, err = sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalln(err)
	}
}

func main() {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"localhost:9092"},
		GroupID:  "file-processing-group",
		Topic:    "file-chunks",
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})

	fmt.Println("Worker Node started. Listening for Kafka chunks...")

	for {
		m, err := r.FetchMessage(ctx)
		if err != nil {
			log.Println("Error fetching message:", err)
			break
		}

		var event models.ChunkEvent
		if err := json.Unmarshal(m.Value, &event); err != nil {
			log.Println("Failed to parse event:", err)
			continue
		}

		fmt.Printf("Processing chunk for Job: %s, Start: %d, End: %d\n", event.JobID, event.StartByte, event.EndByte)

		processChunk(event)

		r.CommitMessages(ctx, m)
	}
}

func processChunk(event models.ChunkEvent) {
	// 1. Fetch exact byte range from MinIO
	opts := minio.GetObjectOptions{}
	opts.SetRange(event.StartByte, event.EndByte)
	
	obj, err := minioClient.GetObject(ctx, "uploads", event.FileName, opts)
	if err != nil {
		log.Println("MinIO Error:", err)
		return
	}
	defer obj.Close()

	buf := new(bytes.Buffer)
	buf.ReadFrom(obj)
	lines := strings.Split(buf.String(), "\n")

	// 2. Aggregate Sales locally in memory
	// In a real scenario, you parse the CSV properly
	// Assuming format: id,region,amount,timestamp
	regionTotals := make(map[string]float64)
	for _, line := range lines {
		parts := strings.Split(line, ",")
		if len(parts) >= 3 {
			region := parts[1]
			// Mocked parsing amount (in real life, strconv.ParseFloat)
			// Let's just add 10.0 for every valid row for demo purposes
			regionTotals[region] += 10.0
		}
	}

	// 3. Atomically Upsert to Postgres
	tx, err := db.Begin()
	if err == nil {
		for region, amount := range regionTotals {
			_, err = tx.Exec(`
				INSERT INTO region_sales (region, total_amount) 
				VALUES ($1, $2) 
				ON CONFLICT (region) 
				DO UPDATE SET total_amount = region_sales.total_amount + $2`, 
				region, amount)
		}
		tx.Commit()
	}

	// 4. Update Progress in Redis
	// Fetch the current job status, increment completed chunks
	val, err := redisClient.Get(ctx, event.JobID).Result()
	if err == nil {
		var status models.JobStatus
		json.Unmarshal([]byte(val), &status)
		status.CompletedChunks++
		if status.CompletedChunks == status.TotalChunks {
			status.Status = "completed"
		}
		statusBytes, _ := json.Marshal(status)
		redisClient.Set(ctx, event.JobID, statusBytes, 24*time.Hour)
	}
}
