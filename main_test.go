package main

import (
	"testing"
	"time"
)

func TestNormalizeQuery(t *testing.T) {
	cases := []struct {
		input string
		want  string
	}{
		{"  IPhone    15   Pro  ", "iphone 15 pro"},
		{"MacBook", "macbook"},
		{"   ", ""},
	}

	for _, c := range cases {
		got := normalizeQuery(c.input)
		if got != c.want {
			t.Errorf("normalizeQuery(%q) = %q, want %q", c.input, got, c.want)
		}
	}
}

func TestAggregatorWindow(t *testing.T) {
	aggregator := NewAggregator()

	now := time.Date(2026, 5, 26, 12, 0, 0, 0, time.UTC)

	// это событие должно выпасть из окна
	aggregator.Add("iphone", now.Add(-6*time.Minute))
	aggregator.Add("iphone", now.Add(-2*time.Minute))
	aggregator.Add("iphone", now.Add(-30*time.Second))
	aggregator.Add("macbook", now.Add(-1*time.Minute))

	items := aggregator.BuildTop(10, now, map[string]struct{}{})

	if len(items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(items))
	}

	if items[0].Query != "iphone" || items[0].Count != 2 {
		t.Errorf("expected iphone:2, got %+v", items[0])
	}
}

func TestAggregatorSorting(t *testing.T) {
	aggregator := NewAggregator()
	now := time.Now()

	aggregator.Add("banana", now)
	aggregator.Add("apple", now)
	aggregator.Add("macbook", now)
	aggregator.Add("macbook", now)

	items := aggregator.BuildTop(10, now, map[string]struct{}{})

	if items[0].Query != "macbook" {
		t.Errorf("expected macbook first, got %s", items[0].Query)
	}

	// при одинаковом count — алфавитный порядок
	if items[1].Query != "apple" {
		t.Errorf("expected apple second, got %s", items[1].Query)
	}
}

func TestAggregatorLimit(t *testing.T) {
	aggregator := NewAggregator()
	now := time.Now()

	aggregator.Add("iphone", now)
	aggregator.Add("macbook", now)
	aggregator.Add("airpods", now)

	items := aggregator.BuildTop(2, now, map[string]struct{}{})
	if len(items) != 2 {
		t.Fatalf("expected 2, got %d", len(items))
	}
}

func TestStopList(t *testing.T) {
	aggregator := NewAggregator()
	stopList := NewStopList()
	now := time.Now()

	aggregator.Add("iphone", now)
	aggregator.Add("iphone 15", now) // тоже должен отфильтроваться, содержит "iphone"
	aggregator.Add("macbook", now)

	stopList.Add("iphone")

	items := aggregator.BuildTop(10, now, stopList.Snapshot())

	if len(items) != 1 || items[0].Query != "macbook" {
		t.Fatalf("expected only macbook, got %+v", items)
	}
}

func TestSpamGuardLimit(t *testing.T) {
	sg := NewSpamGuard()
	now := time.Date(2026, 5, 26, 12, 0, 0, 0, time.UTC)

	for i := 0; i < spamGuardMaxEvents; i++ {
		if !sg.Allow("user:u1", "iphone", now.Add(time.Duration(i)*time.Second)) {
			t.Fatalf("event %d should be allowed", i+1)
		}
	}

	if sg.Allow("user:u1", "iphone", now.Add(10*time.Second)) {
		t.Fatal("should be blocked after limit")
	}
}

func TestSpamGuardIsolation(t *testing.T) {
	sg := NewSpamGuard()
	now := time.Now()

	for i := 0; i < spamGuardMaxEvents; i++ {
		sg.Allow("user:u1", "iphone", now)
	}

	// другой пользователь не должен быть заблокирован
	if !sg.Allow("user:u2", "iphone", now) {
		t.Fatal("different user should not be blocked")
	}
}

func TestSpamGuardWindowReset(t *testing.T) {
	sg := NewSpamGuard()
	now := time.Date(2026, 5, 26, 12, 0, 0, 0, time.UTC)

	for i := 0; i < spamGuardMaxEvents; i++ {
		sg.Allow("user:u1", "iphone", now)
	}

	if !sg.Allow("user:u1", "iphone", now.Add(spamGuardWindow+time.Second)) {
		t.Fatal("should be allowed after window expires")
	}
}

func TestSpamGuardCleanup(t *testing.T) {
	sg := NewSpamGuard()
	now := time.Date(2026, 5, 26, 12, 0, 0, 0, time.UTC)

	sg.Allow("user:u1", "iphone", now)
	sg.Allow("user:u2", "macbook", now)

	sg.Cleanup(now.Add(spamGuardWindow + time.Second))

	if len(sg.events) != 0 {
		t.Fatalf("expected map to be empty after cleanup, got %d keys", len(sg.events))
	}
}

func TestTopCacheGet(t *testing.T) {
	cache := NewTopCache()

	cache.Store([]TrendItem{
		{Query: "iphone", Count: 3},
		{Query: "macbook", Count: 2},
		{Query: "airpods", Count: 1},
	}, time.Now())

	items := cache.Get(2)
	if len(items) != 2 {
		t.Fatalf("expected 2, got %d", len(items))
	}

	if items[0].Query != "iphone" || items[1].Query != "macbook" {
		t.Errorf("wrong order: %+v", items)
	}
}
