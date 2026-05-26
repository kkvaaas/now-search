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
	defaultLimit  = 10
	maxLimit = 100
	windowSeconds = 300
)

var windowDuration = 5 * time.Minute

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
	EventID string `json:"event_id"`
	Query string `json:"query"`
	UserID string `json:"user_id"`
	SessionID string `json:"session_id"`
	CreatedAt string `json:"created_at"`
}

type SearchEventResponse struct {
	Status string `json:"status"`
	Query string `json:"query"`
	CreatedAt string `json:"created_at"`
}

type SearchEvent struct {
	Query string
	CreatedAt time.Time
}

type Aggregator struct {
	mu sync.Mutex
	events []SearchEvent
}

func main() {
	aggregator := NewAggregator()

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

func NewAggregator() *Aggregator {
	return &Aggregator{
		events: make([]SearchEvent, 0),
	}
}

func (a *Aggregator) Add(query string, now time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.events = append(a.events, SearchEvent{
		Query: query,
		CreatedAt: now,
	})
}

func (a *Aggregator) Top(limit int, now time.Time) []TrendItem {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.removeOldEvents(now)

	counts := make(map[string]int)

	for _, event := range a.events {
		counts[event.Query]++
	}

	items := make([]TrendItem, 0, len(counts))

	for query, count := range counts {
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

func (a *Aggregator) removeOldEvents(now time.Time) {
	minAllowedTime := now.Add(-windowDuration)

	actualEvents := a.events[:0]

	for _, event := range a.events {
		if event.CreatedAt.After(minAllowedTime) || event.CreatedAt.Equal(minAllowedTime) {
			actualEvents = append(actualEvents, event)
		}
	}

	a.events = actualEvents
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

		response := TopResponse{
			WindowSeconds: windowSeconds,
			Limit:         limit,
			Items:         aggregator.Top(limit, time.Now()),
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

		query := normalizeQuery(request.Query)
		if query == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{
				"error": "query must not be empty",
			})
			return
		}

		eventTime, err := parseEventTime(request.CreatedAt)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{
				"error": "created_at must be in RFC3339 format",
			})
			return
		}

		aggregator.Add(query, eventTime)

		writeJSON(w, http.StatusCreated, SearchEventResponse{
			Status: "accepted",
			Query: query,
			CreatedAt: eventTime.Format(time.RFC3339),
		})
	}
}

func normalizeQuery(query string) string {
	query = strings.TrimSpace(query)
	query = strings.ToLower(query)
	query = strings.Join(strings.Fields(query), " ")

	return query
}

func parseEventTime(rawTime string) (time.Time, error) {
	rawTime = strings.TrimSpace(rawTime)

	if rawTime == "" {
		return time.Now(), nil
	}

	eventTime, err := time.Parse(time.RFC3339, rawTime)
	if err != nil {
		return time.Time{}, err
	}

	return eventTime, nil
}

func writeJSON(w http.ResponseWriter, statusCode int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)

	if err := json.NewEncoder(w).Encode(data); err != nil {
		log.Println("failed to write json response:", err)
	}
}