package store

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/arishahmad661/execra/internal/metrics"
)

func TestBasicPersistence(t *testing.T) {
	dir := t.TempDir()
	m := metrics.NewMetrics()
	store, err := NewBadgerDb(dir, m)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	id1, _ := store.Enqueue(context.Background(), "message", []byte(`{"msg":"from"}`), 5)
	id2, _ := store.Enqueue(context.Background(), "message", []byte(`{"msg":"me"}`), 5)
	id3, _ := store.Enqueue(context.Background(), "message", []byte(`{"msg":"to"}`), 5)
	id4, _ := store.Enqueue(context.Background(), "message", []byte(`{"msg":"you"}`), 5)

	err = store.Close()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	store, err = NewBadgerDb(dir, m)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	leasedJobs, err := store.Dequeue(context.Background(), "message", "mesenger-worker-1", 5, 30)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	returnedIDs := map[string]bool{}
	for _, lj := range leasedJobs {
		returnedIDs[lj.Job.Id] = true
	}

	if !returnedIDs[id1] {
		t.Error("job 1 missing after restart")
	}
	if !returnedIDs[id2] {
		t.Error("job 2 missing after restart")
	}
	if !returnedIDs[id3] {
		t.Error("job 3 missing after restart")
	}
	if !returnedIDs[id4] {
		t.Error("job 4 missing after restart")
	}

	err = store.Close()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLeaseExpiryAndReassignment(t *testing.T) {
	dir := t.TempDir()
	m := metrics.NewMetrics()
	store, err := NewBadgerDb(dir, m)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var ids []string
	for i := 0; i < 10; i++ {
		id, err := store.Enqueue(context.Background(), "count", []byte(fmt.Sprintf(`"digit": %v`, i)), 5)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		ids = append(ids, id)
	}

	_, err = store.Dequeue(context.Background(), "count", "digit-worker-1", 10, 1)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	time.Sleep(7 * time.Second)
	store.RequeueExpired()

	leasedJobs, err := store.Dequeue(context.Background(), "count", "digit-worker-1", 10, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	returnedIDs := map[string]bool{}
	for _, lj := range leasedJobs {
		returnedIDs[lj.Job.Id] = true
	}

	for _, id := range ids {
		if !returnedIDs[id] {
			t.Errorf("%s job missing after requeue expired", id)
		}
	}

	err = store.Close()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestExponentialBackoff(t *testing.T) {
	dir := t.TempDir()
	m := metrics.NewMetrics()
	store, err := NewBadgerDb(dir, m)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_, err = store.Enqueue(context.Background(), "message", []byte(`"msg": "from"`), 3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	leasedJobs, err := store.Dequeue(context.Background(), "message", "message-worker-1", 1, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, _, err = store.Nack(context.Background(), leasedJobs[0].Job.Id, leasedJobs[0].Lease.LeaseID, "nack for test")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	leasedJobs, err = store.Dequeue(context.Background(), "message", "message-worker-1", 1, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(leasedJobs) != 0 {
		t.Fatalf("unexpected error: %v", err)
	}

	store.RequeueExpired()

	leasedJobs, err = store.Dequeue(context.Background(), "message", "message-worker-1", 1, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(leasedJobs) != 0 {
		t.Fatalf("expected empty queue after backoff, got %d jobs", len(leasedJobs))
	}

	err = store.Close()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDLQAfterMaxRetries(t *testing.T) {
	dir := t.TempDir()
	m := metrics.NewMetrics()
	store, err := NewBadgerDb(dir, m)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_, err = store.Enqueue(context.Background(), "message", []byte(`"msg": "from"`), 3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for i := 0; i < 3; i++ {
		leasedJobs, err := store.Dequeue(context.Background(), "message", "message-worker-1", 1, 1)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(leasedJobs) < 1 {
			t.Fatalf("leased job not found %v", err)
		}
		isDlq, _, err := store.Nack(context.Background(), leasedJobs[0].Job.Id, leasedJobs[0].Lease.LeaseID, "nack for test")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if i == 2 && !isDlq {
			t.Fatalf("not in dlq: %v", err)
		}

		time.Sleep(2 * time.Second)
		store.RequeueExpired()
	}

	leasedJobs, err := store.Dequeue(context.Background(), "message", "message-worker-1", 1, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(leasedJobs) != 0 {
		t.Fatalf("expected empty queue after backoff, got %d jobs", len(leasedJobs))
	}

	err = store.Close()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
