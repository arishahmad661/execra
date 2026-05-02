package store

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
)

type LeasedJob struct {
	Job   *Job
	Lease *Lease
}

type MemoryStore struct {
	jobs     map[string]*Job
	queues   map[string][]string
	inFlight map[string]*Lease

	mu sync.Mutex
}

func NewMemoryStore() Store {
	return &MemoryStore{
		jobs:     make(map[string]*Job),
		queues:   make(map[string][]string),
		inFlight: make(map[string]*Lease),
	}
}

func (s *MemoryStore) Enqueue(ctx context.Context, queue string, payload []byte, maxRetries int32) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	id := uuid.NewString()

	job := &Job{
		Id:         id,
		Queue:      queue,
		Payload:    payload,
		MaxRetries: maxRetries,
		Status:     JobStatusQueued,
	}

	if _, ok := s.queues[queue]; !ok {
		s.queues[queue] = []string{}
	}

	s.jobs[id] = job
	s.queues[queue] = append(s.queues[queue], id)

	return id, nil
}

func (s *MemoryStore) Dequeue(ctx context.Context, queue string, workerId string, maxJobs int32, leaseDuration int64) ([]LeasedJob, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.requeueExpired()

	q, ok := s.queues[queue]
	if !ok || len(q) == 0 {
		return []LeasedJob{}, nil
	}

	now := time.Now().Unix()
	var result []LeasedJob

	for i := 0; i < len(q) && int32(len(result)) < maxJobs; i++ {
		jobID := q[i]
		job := s.jobs[jobID]

		leaseID := uuid.NewString()

		lease := &Lease{
			JobID:    jobID,
			LeaseID:  leaseID,
			WorkerID: workerId,
			Expiry:   now + leaseDuration,
		}

		s.inFlight[jobID] = lease
		job.Status = JobStatusInFlight

		result = append(result, LeasedJob{
			Job:   job,
			Lease: lease,
		})
	}

	// remove dequeued jobs from queue
	s.queues[queue] = q[len(result):]

	return result, nil
}

func (s *MemoryStore) Ack(ctx context.Context, jobId string, leaseId string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	lease, ok := s.inFlight[jobId]
	if !ok {
		return errors.New("invalid lease: job not in flight")
	}

	if lease.LeaseID != leaseId {
		return errors.New("invalid lease: lease ID mismatch")
	}

	job := s.jobs[jobId]
	job.Status = JobStatusCompleted

	delete(s.inFlight, jobId)

	return nil
}

func (s *MemoryStore) Nack(ctx context.Context, jobId string, leaseId string, errMsg string) (bool, int32, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	lease, ok := s.inFlight[jobId]
	if !ok {
		return false, 0, errors.New("invalid lease: job not in flight")
	}

	if lease.LeaseID != leaseId {
		return false, 0, errors.New("invalid lease: lease ID mismatch")
	}

	job := s.jobs[jobId]
	job.AttemptCount++

	delete(s.inFlight, jobId)

	if job.AttemptCount >= job.MaxRetries {
		job.Status = JobStatusFailed
		return true, job.AttemptCount, nil
	}

	job.Status = JobStatusQueued
	s.queues[job.Queue] = append(s.queues[job.Queue], jobId)

	return false, job.AttemptCount, nil
}

func (s *MemoryStore) Schedule(ctx context.Context, queue string, payload []byte, maxRetries int32, executeAt int64) (string, error) {
	// TODO: delayed queue (min-heap)
	return s.Enqueue(ctx, queue, payload, maxRetries)
}

func (s *MemoryStore) requeueExpired() {
	now := time.Now().Unix()

	for jobId, lease := range s.inFlight {
		if lease.Expiry <= now {
			job := s.jobs[jobId]
			job.AttemptCount++

			delete(s.inFlight, jobId)

			if job.AttemptCount > job.MaxRetries {
				job.Status = JobStatusFailed
				continue
			}

			job.Status = JobStatusQueued
			s.queues[job.Queue] = append(s.queues[job.Queue], jobId)
		}
	}
}
