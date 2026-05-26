package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

const (
	defaultLimit  = 10
	maxLimit      = 100
	windowSeconds = 300

	searchEventsSubject = "search.events"

	spamGuardMaxEvents = 3
)

var (
	windowDuration  = 5 * time.Minute
	spamGuardWindow = time.Minute
)

type TrendItem struct {
	Query string `json:"query"`
	Count int    `json:"count"`
}

type TopResponse struct {
	WindowSeconds int         `json:"window_seconds"`
	Limit         int         `json:"limit"`
	Items         []TrendItem `json:"items"`
}

type SearchEventRequest struct {
	EventID   string `json:"event_id"`
	Query     string `json:"query"`
	UserID    string `json:"user_id"`
	SessionID string `json:"session_id"`
	CreatedAt string `json:"created_at"`
}

type SearchEventResponse struct {
	Status    string `json:"status"`
	Query     string `json:"query"`
	CreatedAt string `json:"created_at"`
}

type StopWordRequest struct {
	Word string `json:"word"`
}

type StopWordResponse struct {
	Status string `json:"status"`
	Word   string `json:"word"`
}

type StopListResponse struct {
	Items []string `json:"items"`
}

type SearchEvent struct {
	Query     string
	CreatedAt time.Time
}

type Aggregator struct {
	mu     sync.Mutex
	events []SearchEvent
}

type StopList struct {
	mu    sync.RWMutex
	words map[string]struct{}
}

type SpamGuard struct {
	mu     sync.Mutex
	events map[string][]time.Time
}

func main() {
	aggregator := NewAggregator()
	stopList := NewStopList()
	spamGuard := NewSpamGuard()

	natsConn := connectNATS(aggregator, spamGuard)
	if natsConn != nil {
		defer natsConn.Close()
	}

	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", healthHandler)
	mux.HandleFunc("GET /api/v1/trends/top", topHandler(aggregator, stopList))
	mux.HandleFunc("POST /debug/search-events", addSearchEventHandler(aggregator, spamGuard))

	mux.HandleFunc("GET /api/v1/stop-list", getStopListHandler(stopList))
	mux.HandleFunc("POST /api/v1/stop-list", addStopWordHandler(stopList))
	mux.HandleFunc("DELETE /api/v1/stop-list", deleteStopWordHandler(stopList))

	server := &http.Server{
		Addr:         ":8080",
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	log.Println("server started on :8080")

	if err := server.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}

func connectNATS(aggregator *Aggregator, spamGuard *SpamGuard) *nats.Conn {
	natsURL := getEnv("NATS_URL", nats.DefaultURL)

	natsConn, err := nats.Connect(natsURL)
	if err != nil {
		log.Println("nats is not connected:", err)
		return nil
	}

	_, err = natsConn.Subscribe(searchEventsSubject, searchEventMessageHandler(aggregator, spamGuard))
	if err != nil {
		log.Println("failed to subscribe to nats subject:", err)
		natsConn.Close()
		return nil
	}

	if err := natsConn.Flush(); err != nil {
		log.Println("failed to flush nats subscription:", err)
		natsConn.Close()
		return nil
	}

	log.Println("subscribed to nats subject:", searchEventsSubject)

	return natsConn
}

func searchEventMessageHandler(aggregator *Aggregator, spamGuard *SpamGuard) nats.MsgHandler {
	return func(message *nats.Msg) {
		var request SearchEventRequest

		if err := json.Unmarshal(message.Data, &request); err != nil {
			log.Println("failed to decode nats message:", err)
			return
		}

		query := normalizeQuery(request.Query)
		if query == "" {
			log.Println("empty query in nats message")
			return
		}

		eventTime, err := parseEventTime(request.CreatedAt)
		if err != nil {
			log.Println("invalid created_at in nats message:", err)
			return
		}

		identity := actorIdentity(request)
		if !spamGuard.Allow(identity, query, eventTime) {
			log.Println("search event skipped by spam guard:", query)
			return
		}

		aggregator.Add(query, eventTime)
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
		Query:     query,
		CreatedAt: now,
	})
}

func (a *Aggregator) Top(limit int, now time.Time, stopWords map[string]struct{}) []TrendItem {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.removeOldEvents(now)

	counts := make(map[string]int)

	for _, event := range a.events {
		if isBlockedByStopList(event.Query, stopWords) {
			continue
		}

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

func NewStopList() *StopList {
	return &StopList{
		words: make(map[string]struct{}),
	}
}

func (s *StopList) Add(word string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.words[word] = struct{}{}
}

func (s *StopList) Delete(word string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.words, word)
}

func (s *StopList) List() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	items := make([]string, 0, len(s.words))

	for word := range s.words {
		items = append(items, word)
	}

	sort.Strings(items)

	return items
}

func (s *StopList) Snapshot() map[string]struct{} {
	s.mu.RLock()
	defer s.mu.RUnlock()

	snapshot := make(map[string]struct{}, len(s.words))

	for word := range s.words {
		snapshot[word] = struct{}{}
	}

	return snapshot
}

func NewSpamGuard() *SpamGuard {
	return &SpamGuard{
		events: make(map[string][]time.Time),
	}
}

func (s *SpamGuard) Allow(identity string, query string, now time.Time) bool {
	identity = strings.TrimSpace(identity)
	if identity == "" {
		return true
	}

	key := identity + ":" + query
	minAllowedTime := now.Add(-spamGuardWindow)

	s.mu.Lock()
	defer s.mu.Unlock()

	eventTimes := s.events[key]
	actualTimes := eventTimes[:0]

	for _, eventTime := range eventTimes {
		if eventTime.After(minAllowedTime) || eventTime.Equal(minAllowedTime) {
			actualTimes = append(actualTimes, eventTime)
		}
	}

	if len(actualTimes) >= spamGuardMaxEvents {
		s.events[key] = actualTimes
		return false
	}

	actualTimes = append(actualTimes, now)
	s.events[key] = actualTimes

	return true
}

func actorIdentity(request SearchEventRequest) string {
	userID := strings.TrimSpace(request.UserID)
	if userID != "" {
		return "user:" + userID
	}

	sessionID := strings.TrimSpace(request.SessionID)
	if sessionID != "" {
		return "session:" + sessionID
	}

	return ""
}

func isBlockedByStopList(query string, stopWords map[string]struct{}) bool {
	for word := range stopWords {
		if query == word || strings.Contains(query, word) {
			return true
		}
	}

	return false
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"status": "ok",
	})
}

func topHandler(aggregator *Aggregator, stopList *StopList) http.HandlerFunc {
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

		stopWords := stopList.Snapshot()

		response := TopResponse{
			WindowSeconds: windowSeconds,
			Limit:         limit,
			Items:         aggregator.Top(limit, time.Now(), stopWords),
		}

		writeJSON(w, http.StatusOK, response)
	}
}

func addSearchEventHandler(aggregator *Aggregator, spamGuard *SpamGuard) http.HandlerFunc {
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

		identity := actorIdentity(request)
		if !spamGuard.Allow(identity, query, eventTime) {
			writeJSON(w, http.StatusAccepted, SearchEventResponse{
				Status:    "filtered_by_spam_guard",
				Query:     query,
				CreatedAt: eventTime.Format(time.RFC3339),
			})
			return
		}

		aggregator.Add(query, eventTime)

		writeJSON(w, http.StatusCreated, SearchEventResponse{
			Status:    "accepted",
			Query:     query,
			CreatedAt: eventTime.Format(time.RFC3339),
		})
	}
}

func getStopListHandler(stopList *StopList) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, StopListResponse{
			Items: stopList.List(),
		})
	}
}

func addStopWordHandler(stopList *StopList) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var request StopWordRequest

		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{
				"error": "invalid json body",
			})
			return
		}

		word := normalizeQuery(request.Word)
		if word == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{
				"error": "word must not be empty",
			})
			return
		}

		stopList.Add(word)

		writeJSON(w, http.StatusCreated, StopWordResponse{
			Status: "added",
			Word:   word,
		})
	}
}

func deleteStopWordHandler(stopList *StopList) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		word := normalizeQuery(r.URL.Query().Get("word"))
		if word == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{
				"error": "word query parameter is required",
			})
			return
		}

		stopList.Delete(word)

		writeJSON(w, http.StatusOK, StopWordResponse{
			Status: "deleted",
			Word:   word,
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

func getEnv(key string, defaultValue string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return defaultValue
	}

	return value
}

func writeJSON(w http.ResponseWriter, statusCode int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)

	if err := json.NewEncoder(w).Encode(data); err != nil {
		log.Println("failed to write json response:", err)
	}
}
