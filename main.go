package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/segmentio/kafka-go"
)

type DataPayload struct {
	ID        string  `json:"id"`
	Source    string  `json:"source"`
	EVValue   float64 `json:"ev_value"`
	Payload   string  `json:"payload"`
	Timestamp int64   `json:"timestamp"`
}

var kafkaWriter *kafka.Writer

func init() {
	kafkaWriter = &kafka.Writer{
		Addr:         kafka.TCP("redpanda:9092"),
		Topic:        "ingestion-stream",
		Balancer:     &kafka.LeastBytes{},
		BatchSize:    100,                  // Optimized: Batch up to 100 messages
		BatchTimeout: 5 * time.Millisecond, // Optimized: Flush every 5ms to maintain low latency
		Async:        true,                 // Optimized: Non-blocking writes prevent FD exhaustion
	}
}

// Perimeter filter (Reality Script / Zero-Cost Filtration)
func realityScriptFilter(p DataPayload) bool {
	if p.EVValue <= -6.666 || p.Payload == "" {
		return false // Drop instantly
	}
	return true
}

func ingestHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var payload DataPayload
	err := json.NewDecoder(r.Body).Decode(&payload)
	if err != nil {
		http.Error(w, "Malformed payload", http.StatusBadRequest)
		return
	}

	// Zero-Cost Edge Annihilation
	if !realityScriptFilter(payload) {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	payload.Timestamp = time.Now().UnixNano()
	
	// FIXED: Properly handling the JSON marshal error
	msgBytes, err := json.Marshal(payload)
	if err != nil {
		http.Error(w, "Serialization error", http.StatusInternalServerError)
		return
	}

	// Native async push to Grand Gallery (no need for manual goroutine wrapper now)
	err = kafkaWriter.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte(payload.ID),
			Value: msgBytes,
		},
	)

	if err != nil {
		log.Printf("Failed to queue message: %v", err)
		http.Error(w, "Ingestion buffer error", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte(`{"status":"queued"}`))
}

func main() {
	defer kafkaWriter.Close()
	http.HandleFunc("/ingest", ingestHandler)
	log.Println("Singularity Gateway running on port 8080...")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
