package store

import (
	"context"
	"testing"
)

func TestEnqueueReturnsJobID(t *testing.T) {
	s := NewMemoryStore()

	id, err := s.Enqueue(context.Background(), "test", []byte("data"), 3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if id == "" {
		t.Fatal("expected non-empty job ID")
	}
}

func TestDequeueReturnsLeasedJob(t *testing.T) {
	s := NewMemoryStore()

	_, _ = s.Enqueue(context.Background(), "test", []byte("data"), 3)

	jobs, err := s.Dequeue(context.Background(), "test", "worker-1", 1, 30)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(jobs) != 1 {
		t.Fatalf("expected 1 job, got %d", len(jobs))
	}

	if jobs[0].Lease == nil {
		t.Fatal("expected lease to be present")
	}
}

func TestAckCompletesJob(t *testing.T) {
	s := NewMemoryStore()

	id, _ := s.Enqueue(context.Background(), "test", []byte("data"), 3)

	jobs, _ := s.Dequeue(context.Background(), "test", "worker-1", 1, 30)

	err := s.Ack(context.Background(), id, jobs[0].Lease.LeaseID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestAckWithWrongLeaseReturnsError(t *testing.T) {
	s := NewMemoryStore()

	id, _ := s.Enqueue(context.Background(), "test", []byte("data"), 3)
	_, _ = s.Dequeue(context.Background(), "test", "worker-1", 1, 30)

	err := s.Ack(context.Background(), id, "wrong-lease")
	if err == nil {
		t.Fatal("expected error for wrong lease ID")
	}
}

func TestNackRequeuesJob(t *testing.T) {
	s := NewMemoryStore()

	id, _ := s.Enqueue(context.Background(), "test", []byte("data"), 3)

	jobs, _ := s.Dequeue(context.Background(), "test", "worker-1", 1, 30)

	_, _, err := s.Nack(context.Background(), id, jobs[0].Lease.LeaseID, "fail")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	jobs2, _ := s.Dequeue(context.Background(), "test", "worker-2", 1, 30)

	if len(jobs2) != 1 {
		t.Fatal("expected job to be requeued")
	}
}

func TestNackExceedingMaxRetriesSendsToDLQ(t *testing.T) {
	s := NewMemoryStore()
	id, _ := s.Enqueue(context.Background(), "test", []byte("data"), 2)

	for i := 0; i < 2; i++ {
		jobs, _ := s.Dequeue(context.Background(), "test", "worker-1", 1, 30)
		movedToDLQ, _, err := s.Nack(context.Background(), id, jobs[0].Lease.LeaseID, "fail")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if i == 1 && !movedToDLQ {
			t.Fatal("expected job to be moved to DLQ")
		}
	}
}

func TestEmptyQueueReturnsNothing(t *testing.T) {
	s := NewMemoryStore()

	jobs, err := s.Dequeue(context.Background(), "empty", "worker-1", 1, 30)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(jobs) != 0 {
		t.Fatal("expected no jobs")
	}
}
