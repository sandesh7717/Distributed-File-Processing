package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/segmentio/kafka-go"
	"github.com/sde-project/distributed-file-processing/pkg/models"
)

var (
	minioClient *minio.Client
	redisClient *redis.Client
	kafkaWriter *kafka.Writer
	ctx         = context.Background()
)

const bucketName = "uploads"

func init() {
	// Initialize MinIO
	var err error
	minioClient, err = minio.New("localhost:9000", &minio.Options{
		Creds:  credentials.NewStaticV4("minioadmin", "minioadmin", ""),
		Secure: false,
	})
	if err != nil {
		log.Fatalln("Failed to connect to MinIO:", err)
	}

	// Initialize Redis
	redisClient = redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	// Initialize Kafka Writer
	kafkaWriter = &kafka.Writer{
		Addr:     kafka.TCP("localhost:9092"),
		Topic:    "file-chunks",
		Balancer: &kafka.LeastBytes{},
	}
}

func main() {
	r := gin.Default()

	r.POST("/upload", handleUpload)
	r.GET("/status/:job_id", handleStatus)

	fmt.Println("API Gateway running on :8080")
	r.Run(":8080")
}

func handleUpload(c *gin.Context) {
	file, err := c.FormFile("file")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "No file uploaded"})
		return
	}

	jobID := uuid.New().String()
	fileName := fmt.Sprintf("%s_%s", jobID, file.Filename)

	src, err := file.Open()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to open file"})
		return
	}
	defer src.Close()

	// 1. Upload directly to MinIO (Stream, no memory bloat)
	_, err = minioClient.PutObject(ctx, bucketName, fileName, src, file.Size, minio.PutObjectOptions{
		ContentType: "text/csv",
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to upload to MinIO"})
		return
	}

	// 2. Set Initial Status in Redis
	status := models.JobStatus{
		JobID:          jobID,
		Status:         "processing",
		Progress:       0,
		TotalChunks:    0,
		CompletedChunks: 0,
	}
	
	// Simulated Chunking (In production, we'd read the file to find line offsets)
	// Here we just split it by arbitrary byte sizes or assume a set number of chunks
	chunkSize := int64(1024 * 1024 * 10) // 10MB per chunk
	totalChunks := int(file.Size / chunkSize)
	if file.Size%chunkSize != 0 {
		totalChunks++
	}
	status.TotalChunks = totalChunks

	statusBytes, _ := json.Marshal(status)
	redisClient.Set(ctx, jobID, statusBytes, 24*time.Hour)

	// 3. Publish chunk tasks to Kafka
	go func() {
		for i := 0; i < totalChunks; i++ {
			startByte := int64(i) * chunkSize
			endByte := startByte + chunkSize - 1
			if endByte > file.Size {
				endByte = file.Size
			}

			event := models.ChunkEvent{
				JobID:     jobID,
				FileName:  fileName,
				StartByte: startByte,
				EndByte:   endByte,
			}
			eventBytes, _ := json.Marshal(event)

			kafkaWriter.WriteMessages(ctx, kafka.Message{
				Key:   []byte(jobID),
				Value: eventBytes,
			})
		}
	}()

	c.JSON(http.StatusAccepted, gin.H{
		"message": "File accepted for processing",
		"job_id":  jobID,
	})
}

func handleStatus(c *gin.Context) {
	jobID := c.Param("job_id")
	val, err := redisClient.Get(ctx, jobID).Result()
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Job not found"})
		return
	}

	var status models.JobStatus
	json.Unmarshal([]byte(val), &status)

	// Calculate Progress percentage
	if status.TotalChunks > 0 {
		status.Progress = int((float64(status.CompletedChunks) / float64(status.TotalChunks)) * 100)
	}

	c.JSON(http.StatusOK, status)
}
