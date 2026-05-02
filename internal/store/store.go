package store

import "context"

type JobStatus int

const (
	JobStatusQueued JobStatus = iota
	JobStatusInFlight
	JobStatusCompleted
	JobStatusFailed
)

type Job struct {
	Id           string
	Queue        string
	Payload      []byte
	MaxRetries   int32
	AttemptCount int32
	Status       JobStatus
}

type Lease struct {
	JobID    string
	LeaseID  string
	WorkerID string
	Expiry   int64
}

type Store interface {
	Enqueue(ctx context.Context, queue string, payload []byte, maxRetries int32) (string, error)
	Dequeue(ctx context.Context, queue string, workerId string, maxJobs int32, leaseDuration int64) ([]LeasedJob, error)
	Ack(ctx context.Context, jobId string, leaseId string) error
	Nack(ctx context.Context, jobId string, leaseId string, errMsg string) (bool, int32, error)
	Schedule(ctx context.Context, queue string, payload []byte, maxRetries int32, executeAt int64) (string, error)
}
