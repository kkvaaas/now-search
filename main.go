package main

import (
	"encoding/json"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	windowSeconds = 300
	defaultLimit  = 10
	maxLimit = 100
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

type SearchEventRequest struct {
	Query string `json:"query"`
}

type SearchEventResponse struct {
	Status string `json:"status"`
	Query string `json:"query"`
}

type bucket struct {
	UnixSecond int64
	Counts map[string]int
}

type Aggregator struct {
	mu sync.Mutex
	window  int
	buckets []bucket
	totals  map[string]int
}

func main() {
	aggregator := NewAggregator(windowSeconds)

	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", healthHandler)
	mux.HandleFunc("GET /api/v1/trends/top", topHandler(aggregator))
	mux.HandleFunc("POST /debug/search-events", addSearchEventHandler(aggregator))

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

func NewAggregator(window int) *Aggregator {
	return &Aggregator{
		window:  window,
		buckets: make([]bucket, window),
		totals:  make(map[string]int),
	}
}

func (a *Aggregator) Add(query string, now time.Time) bool {
	normalizedQuery := normalizeQuery(query)
	if normalizedQuery == "" {
		return false
	}

	currentSecond := now.Unix()

	a.mu.Lock()
	defer a.mu.Unlock()

	a.cleanupExpiredBuckets(currentSecond)

	bucketIndex := int(currentSecond % int64(a.window))
	currentBucket := &a.buckets[bucketIndex]

	if currentBucket.Counts == nil {
		currentBucket.Counts = make(map[string]int)
	}

	if currentBucket.UnixSecond != currentSecond {
		a.clearBucket(currentBucket)
		currentBucket.UnixSecond = currentSecond
	}

	currentBucket.Counts[normalizedQuery]++
	a.totals[normalizedQuery]++

	return true
}

func (a *Aggregator) Top(limit int, now time.Time) []TrendItem {
	if limit <= 0 {
		return []TrendItem{}
	}

	currentSecond := now.Unix()

	a.mu.Lock()
	defer a.mu.Unlock()

	a.cleanupExpiredBuckets(currentSecond)

	items := make([]TrendItem, 0, len(a.totals))

	for query, count := range a.totals {
		items = append(items, TrendItem{
			Query: query,
			Count: count,
		})
	}

	sort.Slice(items, func(i, j int) bool {
		if items[i].Count == items[j].Count {
			return items[i].Query < items[j].Query
		}

		return items[i].Count > items[j].Count
	})

	if limit > len(items) {
		limit = len(items)
	}

	return items[:limit]
}

func (a *Aggregator) cleanupExpiredBuckets(currentSecond int64) {
	minAllowedSecond := currentSecond - int64(a.window) + 1

	for i := range a.buckets {
		currentBucket := &a.buckets[i]

		if currentBucket.Counts == nil {
			continue
		}

		if currentBucket.UnixSecond < minAllowedSecond {
			a.clearBucket(currentBucket)
			currentBucket.UnixSecond = 0
		}
	}
}

func (a *Aggregator) clearBucket(b *bucket) {
	for query, count := range b.Counts {
		a.totals[query] -= count

		if a.totals[query] <= 0 {
			delete(a.totals, query)
		}
	}

	b.Counts = make(map[string]int)
}

func normalizeQuery(query string) string {
	query = strings.TrimSpace(query)
	query = strings.ToLower(query)
	query = strings.Join(strings.Fields(query), " ")

	if len(query) > 200 {
		query = query[:200]
	}

	return query
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"status": "ok",
	})
}

func topHandler(aggregator *Aggregator) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		limit := defaultLimit

		limitRaw := r.URL.Query().Get("limit")
		if limitRaw != "" {
			parsedLimit, err := strconv.Atoi(limitRaw)
			if err != nil || parsedLimit <= 0 {
				writeJSON(w, http.StatusBadRequest, map[string]string{
					"error": "limit must be a positive integer",
				})
				return
			}

			if parsedLimit > maxLimit {
				writeJSON(w, http.StatusBadRequest, map[string]string{
					"error": "limit is too large",
				})
				return
			}

			limit = parsedLimit
		}

		items := aggregator.Top(limit, time.Now())

		response := TopResponse{
			WindowSeconds: windowSeconds,
			Limit:         limit,
			Items:         items,
		}

		writeJSON(w, http.StatusOK, response)
	}
}

func addSearchEventHandler(aggregator *Aggregator) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var request SearchEventRequest

		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{
				"error": "invalid json body",
			})
			return
		}

		normalizedQuery := normalizeQuery(request.Query)
		if normalizedQuery == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{
				"error": "query must not be empty",
			})
			return
		}

		aggregator.Add(normalizedQuery, time.Now())

		writeJSON(w, http.StatusCreated, SearchEventResponse{
			Status: "accepted",
			Query:  normalizedQuery,
		})
	}
}

func writeJSON(w http.ResponseWriter, statusCode int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)

	if err := json.NewEncoder(w).Encode(data); err != nil {
		log.Println("failed to write json response:", err)
	}
}