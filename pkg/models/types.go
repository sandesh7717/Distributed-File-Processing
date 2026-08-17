package models

import "time"

// ChunkEvent represents the message sent to Kafka
type ChunkEvent struct {
	JobID      string `json:"job_id"`
	FileName   string `json:"file_name"`
	StartByte  int64  `json:"start_byte"`
	EndByte    int64  `json:"end_byte"`
	TotalLines int    `json:"total_lines"`
}

// SalesRecord represents the parsed CSV row
type SalesRecord struct {
	ID        int       `json:"id"`
	Region    string    `json:"region"`
	Amount    float64   `json:"amount"`
	Timestamp time.Time `json:"timestamp"`
}

// JobStatus represents the state of the overall job in Redis
type JobStatus struct {
	JobID          string `json:"job_id"`
	Status         string `json:"status"` // "processing", "completed", "failed"
	Progress       int    `json:"progress"`
	TotalChunks    int    `json:"total_chunks"`
	CompletedChunks int    `json:"completed_chunks"`
}
