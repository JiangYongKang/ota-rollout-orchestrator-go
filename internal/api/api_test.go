package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/highcumontoa/ota-rollout-orchestrator-go/internal/domain"
	"github.com/highcumontoa/ota-rollout-orchestrator-go/internal/fleet"
	"github.com/highcumontoa/ota-rollout-orchestrator-go/internal/orchestrator"
	"github.com/highcumontoa/ota-rollout-orchestrator-go/internal/store"
)

func setup(t *testing.T) (string, http.Handler, func()) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "state.json")
	st, err := store.New(path)
	if err != nil {
		t.Fatal(err)
	}
	fl := fleet.New(fleet.WithDelay(func() time.Duration { return time.Millisecond }))
	clk := func() time.Time { return time.Now() }
	o := orchestrator.New(st, fl, orchestrator.Config{
		BatchWait: 100 * time.Millisecond, SuccessRatio: 0.8,
	}, clk)
	fl.SetSink(httpSink{o})
	if err := o.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	return path, NewServer(o), func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = o.Shutdown(ctx)
	}
}

type httpSink struct{ o orchestrator.Orchestrator }

func (s httpSink) Deliver(ctx context.Context, r domain.Report) error {
	_, err := s.o.HandleReport(ctx, r)
	return err
}

func do(t *testing.T, h http.Handler, method, path string, body any) (int, map[string]any) {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		_ = json.NewEncoder(&buf).Encode(body)
	}
	req := httptest.NewRequest(method, path, &buf)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	out := map[string]any{}
	if rec.Code != http.StatusNoContent && rec.Body.Len() > 0 {
		_ = json.Unmarshal(rec.Body.Bytes(), &out)
	}
	return rec.Code, out
}

func TestHTTPEndToEnd(t *testing.T) {
	_, h, shutdown := setup(t)
	defer shutdown()

	for i := 0; i < 4; i++ {
		code, _ := do(t, h, "POST", "/v1/devices", map[string]string{
			"id": "d" + string(rune('0'+i)), "version": "v1.0",
		})
		if code != http.StatusCreated {
			t.Fatalf("create device code=%d", code)
		}
	}

	code, body := do(t, h, "POST", "/v1/rollouts", map[string]any{
		"id": "r1", "target_version": "v2.0",
		"device_ids":  []string{"d0", "d1", "d2", "d3"},
		"batch_sizes": []int{2, 2},
	})
	if code != http.StatusCreated {
		t.Fatalf("create rollout code=%d body=%v", code, body)
	}

	// Poll through HTTP until completion.
	deadline := time.Now().Add(3 * time.Second)
	var phase string
	for time.Now().Before(deadline) {
		code, b := do(t, h, "GET", "/v1/rollouts/r1", nil)
		if code != http.StatusOK {
			t.Fatalf("get rollout %d", code)
		}
		phase, _ = b["phase"].(string)
		if phase == "COMPLETED" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if phase != "COMPLETED" {
		t.Fatalf("phase=%s", phase)
	}

	code, ps := do(t, h, "GET", "/v1/rollouts/r1/participants", nil)
	if code != http.StatusOK {
		t.Fatalf("participants %d", code)
	}
	arr, _ := ps[""].([]any) // unused guard; decode below via re-query
	_ = arr
	code, evs := do(t, h, "GET", "/v1/rollouts/r1/events", nil)
	if code != http.StatusOK {
		t.Fatalf("events %d", code)
	}
	if ea, ok := evs[""].([]any); ok {
		t.Fatalf("unexpected shape: %v", ea)
	}
	// Events decode as a top-level array; fetch raw to assert non-empty.
	req := httptest.NewRequest("GET", "/v1/rollouts/r1/events", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	var rawEvents []map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &rawEvents); err != nil {
		t.Fatalf("events decode: %v", err)
	}
	if len(rawEvents) == 0 {
		t.Fatal("expected audit events")
	}
	t.Logf("audit events recorded: %d (first=%s last=%s)",
		len(rawEvents), rawEvents[0]["type"], rawEvents[len(rawEvents)-1]["type"])
}

func TestHTTPConflictAndNotFound(t *testing.T) {
	_, h, shutdown := setup(t)
	defer shutdown()

	do(t, h, "POST", "/v1/devices", map[string]string{"id": "x", "version": "v1.0"})
	do(t, h, "POST", "/v1/rollouts", map[string]any{
		"id": "r", "target_version": "v2.0",
		"device_ids": []string{"x"}, "batch_sizes": []int{1},
	})
	// Duplicate rollout -> 409.
	code, body := do(t, h, "POST", "/v1/rollouts", map[string]any{
		"id": "r", "target_version": "v2.0",
		"device_ids": []string{"x"}, "batch_sizes": []int{1},
	})
	if code != http.StatusConflict {
		t.Fatalf("duplicate code=%d body=%v", code, body)
	}
	// Missing rollout -> 404.
	code, _ = do(t, h, "GET", "/v1/rollouts/nope", nil)
	if code != http.StatusNotFound {
		t.Fatalf("missing code=%d want 404", code)
	}
	// Malformed JSON -> 400.
	req := httptest.NewRequest("POST", "/v1/reports", bytes.NewBufferString("{bad"))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("bad json code=%d", rec.Code)
	}
}

// TestHTTPConcurrentRolloutCreate: exactly one of many concurrent creates with
// the same id may win.
func TestHTTPConcurrentRolloutCreate(t *testing.T) {
	_, h, shutdown := setup(t)
	defer shutdown()
	do(t, h, "POST", "/v1/devices", map[string]string{"id": "x", "version": "v1.0"})

	var wg sync.WaitGroup
	codes := make(chan int, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := httptest.NewRequest("POST", "/v1/rollouts",
				bytes.NewBufferString(`{"id":"rc","target_version":"v2.0","device_ids":["x"],"batch_sizes":[1]}`))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			codes <- rec.Code
		}()
	}
	wg.Wait()
	close(codes)
	created, conflict := 0, 0
	for c := range codes {
		switch c {
		case http.StatusCreated:
			created++
		case http.StatusConflict:
			conflict++
		default:
			t.Fatalf("unexpected code %d", c)
		}
	}
	if created != 1 || conflict != 19 {
		t.Fatalf("created=%d conflict=%d want 1/19", created, conflict)
	}
}

// TestHTTPSimulateOfflineReconnect: a device taken offline before rollout
// dispatch is counted OFFLINE (missing report never succeeds) and the batch
// fails the strict gate; this exercises the simulation control endpoint.
func TestHTTPSimulateOfflineReconnect(t *testing.T) {
	_, h, shutdown := setup(t)
	defer shutdown()

	for _, id := range []string{"o0", "o1"} {
		if code, _ := do(t, h, "POST", "/v1/devices", map[string]string{
			"id": id, "version": "v1.0",
		}); code != http.StatusCreated {
			t.Fatalf("add %s code=%d", id, code)
		}
	}
	if code, b := do(t, h, "POST", "/v1/devices/o1/simulate", map[string]bool{
		"online": false,
	}); code != http.StatusOK {
		t.Fatalf("offline simulate code=%d body=%v", code, b)
	}
	if code, _ := do(t, h, "POST", "/v1/rollouts", map[string]any{
		"id": "offline-r", "target_version": "v2.0",
		"device_ids":  []string{"o0", "o1"},
		"batch_sizes": []int{2}, "success_ratio": 1.0,
	}); code != http.StatusCreated {
		t.Fatalf("create code=%d", code)
	}

	deadline := time.Now().Add(3 * time.Second)
	var phase string
	for time.Now().Before(deadline) {
		_, b := do(t, h, "GET", "/v1/rollouts/offline-r", nil)
		phase, _ = b["phase"].(string)
		if phase == "FAILED" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if phase != "FAILED" {
		t.Fatalf("phase=%s want FAILED (offline device blocks strict gate)", phase)
	}
	req := httptest.NewRequest("GET", "/v1/rollouts/offline-r/participants", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	var ps []map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &ps); err != nil {
		t.Fatal(err)
	}
	states := map[string]string{}
	for _, p := range ps {
		states[p["device_id"].(string)] = p["state"].(string)
	}
	// o1 never confirmed anything and never came back; within the rollback
	// window it is queued ROLLBACK_FAILED (manual), staying on its baseline.
	if s := states["o1"]; s != "ROLLBACK_FAILED" && s != "ROLLING_BACK" {
		t.Fatalf("offline device state=%s", s)
	}
	t.Logf("offline device o1=%s (baseline retained; ROLLBACK_FAILED => manual backlog)",
		states["o1"])
}
