package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
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
		BatchSize:    100,
		BatchTimeout: 5 * time.Millisecond,
		Async:        true,
	}
}

func realityScriptFilter(p DataPayload) bool {
	return p.EVValue > -6.666 && p.Payload != ""
}

func ingestHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var payload DataPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "Malformed payload", http.StatusBadRequest)
		return
	}

	if !realityScriptFilter(payload) {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	payload.Timestamp = time.Now().UnixNano()
	msgBytes, err := json.Marshal(payload)
	if err != nil {
		http.Error(w, "Serialization error", http.StatusInternalServerError)
		return
	}

	err = kafkaWriter.WriteMessages(context.Background(), kafka.Message{Key: []byte(payload.ID), Value: msgBytes})
	if err != nil {
		http.Error(w, "Ingestion buffer error", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte(`{"status":"queued"}`))
}

func main() {
	server := &http.Server{Addr: ":8080"}
	http.HandleFunc("/ingest", ingestHandler)

	go func() {
		log.Println("Singularity Gateway running on port 8080...")
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	log.Println("Shutting down Gateway gracefully...")
	kafkaWriter.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	server.Shutdown(ctx)
}
