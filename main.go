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
		Addr:     kafka.TCP("redpanda:9092"),
		Topic:    "ingestion-stream",
		Balancer: &kafka.LeastBytes{},
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
	msgBytes, _ := json.Marshal(payload)

	// Async non-blocking push to Grand Gallery
	go func(msg []byte) {
		_ = kafkaWriter.WriteMessages(context.Background(),
			kafka.Message{
				Value: msg,
			},
		)
	}(msgBytes)

	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte(`{"status":"queued"}`))
}

func main() {
	http.HandleFunc("/ingest", ingestHandler)
	log.Println("Singularity Gateway running on port 8080...")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

