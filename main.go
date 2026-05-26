package main

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"time"
)

type TrendItem struct {
	Query string `json:"query"`
	Count int `json:"count"`
}

type TopResponse struct {
	WindowSeconds int `json:"window_seconds"`
	Limit         int `json:"limit"`
	Items         []TrendItem `json:"items"`
}

func main() {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", healthHandler)
	mux.HandleFunc("GET /api/v1/trends/top", topHandler)

	server := &http.Server{
		Addr: ":8080",
		Handler: mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	log.Println("server started on :8080")

	if err := server.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"status": "ok",
	})
}

func topHandler(w http.ResponseWriter, r *http.Request) {
	limit := 10

	limitRaw := r.URL.Query().Get("limit")
	if limitRaw != "" {
		parsedLimit, err := strconv.Atoi(limitRaw)
		if err != nil || parsedLimit <= 0 {
			writeJSON(w, http.StatusBadRequest, map[string]string{
				"error": "limit must be a positive integer",
			})
			return
		}

		limit = parsedLimit
	}

	response := TopResponse{
		WindowSeconds: 300,
		Limit: limit,
		Items: []TrendItem{},
	}

	writeJSON(w, http.StatusOK, response)
}

func writeJSON(w http.ResponseWriter, statusCode int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)

	if err := json.NewEncoder(w).Encode(data); err != nil {
		log.Println("failed to write json response:", err)
	}
}