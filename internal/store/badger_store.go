package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/arishahmad661/execra/internal/metrics"
	"github.com/dgraph-io/badger"
	"github.com/oklog/ulid/v2"
)

type BadgerStore struct {
	db      *badger.DB
	metrics *metrics.Metrics
	stop    chan struct{}
}

const jobQueueKey = "job"
const readyQueueKey = "queue:ready"
const inflightKeyQueueKey = "queue:inflight"
const dlqQueueKey = "queue:dlq"
const leaseQueueKey = "lease"
const scheduleQueueKey = "queue:schedule"

func NewBadgerDb(dir string, m *metrics.Metrics) (Store, error) {
	db, err := badger.Open(badger.DefaultOptions(dir))
	if err != nil {
		return nil, err
	}

	store := &BadgerStore{db: db, stop: make(chan struct{}), metrics: m}
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				store.RequeueExpired()
			case <-store.stop:
				return
			}
		}
	}()
	return store, nil
}

func (s *BadgerStore) Close() error {
	close(s.stop)
	err := s.db.Close()
	if err != nil {
		s.metrics.StoreErrorsTotal.WithLabelValues(metrics.OperationClose, metrics.ErrorBadgerClose).Inc()
		return err
	}
	return nil
}

func (s *BadgerStore) Enqueue(ctx context.Context, queue string, payload []byte, maxRetries int32) (string, error) {
	start := time.Now()
	defer func() {
		s.metrics.EnqueueLatency.WithLabelValues(queue).Observe(time.Since(start).Seconds())
	}()

	var id string
	var err error

	id = ulid.Make().String()
	jobKey := []byte(fmt.Sprintf("%s:%s", jobQueueKey, id))
	queueKey := []byte(fmt.Sprintf("%s:%s:%s", readyQueueKey, queue, id))
	now := time.Now().Unix()

	job := &Job{
		Id:         id,
		Payload:    payload,
		MaxRetries: maxRetries,
		Queue:      queue,
		Status:     JobStatusQueued,
		RetryAfter: now,
	}

	data, err := json.Marshal(job)

	if err != nil {
		s.metrics.EnqueueErrorsTotal.WithLabelValues(queue, metrics.ErrorMarshal).Inc()
		return "", err
	}

	err = s.db.Update(func(txn *badger.Txn) error {
		err := txn.Set(jobKey, []byte(data))
		if err != nil {
			s.metrics.EnqueueErrorsTotal.WithLabelValues(queue, metrics.ErrorBadgerSet).Inc()
			return err
		}

		err = txn.Set(queueKey, []byte{})
		if err != nil {
			s.metrics.EnqueueErrorsTotal.WithLabelValues(queue, metrics.ErrorBadgerSet).Inc()
			return err
		}
		return nil
	})

	if err != nil {
		return "", err
	}

	s.metrics.EnqueueTotal.WithLabelValues(queue).Inc()
	s.metrics.QueueDepth.WithLabelValues(queue, metrics.QueueStateReady).Inc()

	return id, err
}

func (s *BadgerStore) Dequeue(ctx context.Context, queue string, workerId string, maxJobs int32, leaseDuration int64) ([]LeasedJob, error) {
	start := time.Now()
	defer func() {
		s.metrics.DequeueLatency.WithLabelValues(queue).Observe(time.Since(start).Seconds())
	}()

	var leasedJobs []LeasedJob
	getQueueKeyPrefix := []byte(fmt.Sprintf("%s:%s:", readyQueueKey, queue))
	now := time.Now().Unix()

	err := s.db.Update(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)

		defer it.Close()
		var count int32 = 0

		for it.Seek(getQueueKeyPrefix); it.ValidForPrefix(getQueueKeyPrefix); it.Next() {
			if count >= maxJobs {
				break
			}
			count++

			key := it.Item().KeyCopy(nil)
			parts := strings.Split(string(key), ":")
			jobId := parts[len(parts)-1]

			jobKey := []byte(fmt.Sprintf("%s:%s", jobQueueKey, jobId))

			item, err := txn.Get(jobKey)
			if err != nil {
				s.metrics.DequeueErrorsTotal.WithLabelValues(queue, metrics.ErrorBadgerGet).Inc()
				return err
			}

			var payload []byte
			err = item.Value(func(val []byte) error {
				payload = append([]byte{}, val...)
				return nil
			})
			if err != nil {
				s.metrics.DequeueErrorsTotal.WithLabelValues(queue, metrics.ErrorBadgerGet).Inc()
				return err
			}

			id := ulid.Make().String()
			var job *Job

			err = json.Unmarshal(payload, &job)
			if err != nil {
				s.metrics.DequeueErrorsTotal.WithLabelValues(queue, metrics.ErrorUnmarshal).Inc()
				return err
			}

			job = &Job{
				Id:           job.Id,
				Payload:      job.Payload,
				MaxRetries:   job.MaxRetries,
				Queue:        job.Queue,
				Status:       JobStatusInFlight,
				RetryAfter:   job.RetryAfter,
				AttemptCount: job.AttemptCount,
			}

			if job.RetryAfter > now {
				continue
			}

			marshalJob, err := json.Marshal(job)
			if err != nil {
				s.metrics.DequeueErrorsTotal.WithLabelValues(queue, metrics.ErrorMarshal).Inc()
				return err
			}

			err = txn.Set(jobKey, []byte(marshalJob))
			if err != nil {
				s.metrics.DequeueErrorsTotal.WithLabelValues(queue, metrics.ErrorBadgerSet).Inc()
				return err
			}

			lease := &Lease{
				JobID:    jobId,
				LeaseID:  id,
				WorkerID: workerId,
				Expiry:   now + leaseDuration,
			}

			leaseJob := &LeasedJob{
				Job:   job,
				Lease: lease,
			}

			leaseMarshalJob, err := json.Marshal(lease)
			if err != nil {
				s.metrics.DequeueErrorsTotal.WithLabelValues(queue, metrics.ErrorMarshal).Inc()
				return err
			}

			leasedJobs = append(leasedJobs, *leaseJob)

			leaseKey := []byte(fmt.Sprintf("%s:%s", leaseQueueKey, jobId))

			err = txn.Set(leaseKey, []byte(leaseMarshalJob))
			if err != nil {
				s.metrics.DequeueErrorsTotal.WithLabelValues(queue, metrics.ErrorBadgerSet).Inc()
				return err
			}

			queueInFlightKey := []byte(fmt.Sprintf("%s:%s:%s", inflightKeyQueueKey, queue, jobId))
			s.metrics.InflightJobs.WithLabelValues(queue, metrics.QueueStateInflight).Inc()

			err = txn.Set(queueInFlightKey, []byte{})
			if err != nil {
				s.metrics.DequeueErrorsTotal.WithLabelValues(queue, metrics.ErrorBadgerSet).Inc()
				return err
			}
			err = txn.Delete(key)
			if err != nil {
				s.metrics.DequeueErrorsTotal.WithLabelValues(queue, metrics.ErrorBadgerDelete).Inc()
				return err
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}
	s.metrics.QueueDepth.WithLabelValues(queue, metrics.QueueStateReady).Dec()

	for range leasedJobs {
		s.metrics.DequeueTotal.WithLabelValues(queue).Inc()
	}
	return leasedJobs, nil
}

func (s *BadgerStore) Ack(ctx context.Context, jobId string, leaseId string) error {
	start := time.Now()
	var queue string

	err := s.db.Update(func(txn *badger.Txn) error {

		lease, err := txn.Get([]byte(fmt.Sprintf("%s:%s", leaseQueueKey, jobId)))
		if err != nil {
			s.metrics.AckErrorsTotal.WithLabelValues("unknown", metrics.ErrorBadgerGet).Inc()
			return err
		}

		var leasePayload []byte
		err = lease.Value(func(val []byte) error {
			leasePayload = append([]byte{}, val...)
			return nil
		})
		if err != nil {
			s.metrics.AckErrorsTotal.WithLabelValues("unknown", metrics.ErrorBadgerGet).Inc()
			return err
		}

		var marshalLease *Lease
		err = json.Unmarshal(leasePayload, &marshalLease)
		if err != nil {
			s.metrics.AckErrorsTotal.WithLabelValues("unknown", metrics.ErrorUnmarshal).Inc()
			return err
		}

		if marshalLease.LeaseID != leaseId {
			s.metrics.AckErrorsTotal.WithLabelValues("unknown", metrics.ErrorLeaseMismatch).Inc()
			return errors.New("invalid lease: lease ID mismatch")
		}

		job, err := txn.Get([]byte(fmt.Sprintf("%s:%s", jobQueueKey, jobId)))
		if err != nil {
			s.metrics.AckErrorsTotal.WithLabelValues("unknown", metrics.ErrorBadgerGet).Inc()
			return err
		}

		var jobPayload []byte
		err = job.Value(func(val []byte) error {
			jobPayload = append([]byte{}, val...)
			return nil
		})
		if err != nil {
			s.metrics.AckErrorsTotal.WithLabelValues("unknown", metrics.ErrorBadgerGet).Inc()
			return err
		}

		var marshalJob *Job
		err = json.Unmarshal(jobPayload, &marshalJob)
		if err != nil {
			s.metrics.AckErrorsTotal.WithLabelValues("unknown", metrics.ErrorUnmarshal).Inc()
			return err
		}

		queue = marshalJob.Queue

		marshalJob.Status = JobStatusCompleted

		updatedJob, err := json.Marshal(marshalJob)
		if err != nil {
			s.metrics.AckErrorsTotal.WithLabelValues(queue, metrics.ErrorMarshal).Inc()
			return err
		}

		err = txn.Set([]byte(fmt.Sprintf("%s:%s", jobQueueKey, jobId)), []byte(updatedJob))
		if err != nil {
			s.metrics.AckErrorsTotal.WithLabelValues(queue, metrics.ErrorBadgerSet).Inc()
			return err
		}

		err = txn.Delete([]byte(fmt.Sprintf("%s:%s", leaseQueueKey, jobId)))
		if err != nil {
			s.metrics.AckErrorsTotal.WithLabelValues(queue, metrics.ErrorBadgerDelete).Inc()
			return err
		}

		err = txn.Delete([]byte(fmt.Sprintf("%s:%s:%s", inflightKeyQueueKey, marshalJob.Queue, jobId)))
		if err != nil {
			s.metrics.AckErrorsTotal.WithLabelValues(queue, metrics.ErrorBadgerDelete).Inc()
			return err
		}

		s.metrics.InflightJobs.WithLabelValues(queue, metrics.QueueStateInflight).Dec()

		return nil
	})

	if err != nil {
		return err
	}

	s.metrics.AckTotal.WithLabelValues(queue).Inc()
	s.metrics.AckLatency.WithLabelValues(queue).Observe(time.Since(start).Seconds())

	return nil
}

func (s *BadgerStore) Nack(ctx context.Context, jobId string, leaseId string, errMsg string) (bool, int32, error) {
	start := time.Now()
	var queue string

	var attemptCount int32
	isDlq := false

	err := s.db.Update(func(txn *badger.Txn) error {
		lease, err := txn.Get([]byte(fmt.Sprintf("%s:%s", leaseQueueKey, jobId)))
		if err != nil {
			s.metrics.NackErrorsTotal.WithLabelValues("unknown", metrics.ErrorBadgerGet).Inc()
			return err
		}

		var leasePayload []byte
		err = lease.Value(func(val []byte) error {
			leasePayload = append([]byte{}, val...)
			return nil
		})
		if err != nil {
			s.metrics.NackErrorsTotal.WithLabelValues("unknown", metrics.ErrorBadgerGet).Inc()
			return err
		}

		var marshalLease *Lease
		err = json.Unmarshal(leasePayload, &marshalLease)
		if err != nil {
			s.metrics.NackErrorsTotal.WithLabelValues("unknown", metrics.ErrorUnmarshal).Inc()
			return err
		}

		if marshalLease.LeaseID != leaseId {
			s.metrics.NackErrorsTotal.WithLabelValues("unknown", metrics.ErrorLeaseMismatch).Inc()
			return errors.New("invalid lease: lease ID mismatch")
		}

		job, err := txn.Get([]byte(fmt.Sprintf("%s:%s", jobQueueKey, jobId)))
		if err != nil {
			s.metrics.NackErrorsTotal.WithLabelValues("unknown", metrics.ErrorBadgerGet).Inc()
			return err
		}

		var jobPayload []byte
		err = job.Value(func(val []byte) error {
			jobPayload = append([]byte{}, val...)
			return nil
		})
		if err != nil {
			s.metrics.NackErrorsTotal.WithLabelValues("unknown", metrics.ErrorBadgerGet).Inc()
			return err
		}

		var marshalJob *Job
		err = json.Unmarshal(jobPayload, &marshalJob)
		if err != nil {
			s.metrics.NackErrorsTotal.WithLabelValues("unknown", metrics.ErrorUnmarshal).Inc()
			return err
		}

		queue = marshalJob.Queue

		marshalJob.AttemptCount = marshalJob.AttemptCount + 1

		if marshalJob.AttemptCount > 0 {
			s.metrics.RetryTotal.WithLabelValues(queue).Inc()
		}

		if marshalJob.AttemptCount >= marshalJob.MaxRetries {
			marshalJob.Status = JobStatusFailed
			isDlq = true
			attemptCount = marshalJob.AttemptCount

			updatedJob, err := json.Marshal(marshalJob)
			if err != nil {
				s.metrics.NackErrorsTotal.WithLabelValues(queue, metrics.ErrorMarshal).Inc()
				return err
			}

			err = txn.Set([]byte(fmt.Sprintf("%s:%s", jobQueueKey, jobId)), []byte(updatedJob))
			if err != nil {
				s.metrics.NackErrorsTotal.WithLabelValues(queue, metrics.ErrorBadgerSet).Inc()
				return err
			}

			err = txn.Delete([]byte(fmt.Sprintf("%s:%s", leaseQueueKey, jobId)))
			if err != nil {
				s.metrics.NackErrorsTotal.WithLabelValues(queue, metrics.ErrorBadgerDelete).Inc()
				return err
			}

			err = txn.Delete([]byte(fmt.Sprintf("%s:%s:%s", inflightKeyQueueKey, marshalJob.Queue, jobId)))
			if err != nil {
				s.metrics.NackErrorsTotal.WithLabelValues(queue, metrics.ErrorBadgerDelete).Inc()
				return err
			}

			err = txn.Set([]byte(fmt.Sprintf("%s:%s:%s", dlqQueueKey, marshalJob.Queue, jobId)), []byte{})
			if err != nil {
				s.metrics.NackErrorsTotal.WithLabelValues(queue, metrics.ErrorBadgerSet).Inc()
				return err
			}

			s.metrics.InflightJobs.WithLabelValues(queue, metrics.QueueStateInflight).Dec()
			s.metrics.DLQSize.WithLabelValues(queue).Inc()

			return nil
		}

		marshalJob.Status = JobStatusQueued
		attemptCount = marshalJob.AttemptCount

		base := time.Now().Unix() + int64(1)
		marshalJob.RetryAfter = base

		updatedJob, err := json.Marshal(marshalJob)
		if err != nil {
			s.metrics.NackErrorsTotal.WithLabelValues(queue, metrics.ErrorMarshal).Inc()
			return err
		}

		err = txn.Set([]byte(fmt.Sprintf("%s:%s", jobQueueKey, jobId)), []byte(updatedJob))
		if err != nil {
			s.metrics.NackErrorsTotal.WithLabelValues(queue, metrics.ErrorBadgerSet).Inc()
			return err
		}

		err = txn.Delete([]byte(fmt.Sprintf("%s:%s", leaseQueueKey, jobId)))
		if err != nil {
			s.metrics.NackErrorsTotal.WithLabelValues(queue, metrics.ErrorBadgerDelete).Inc()
			return err
		}

		err = txn.Delete([]byte(fmt.Sprintf("%s:%s:%s", inflightKeyQueueKey, marshalJob.Queue, jobId)))
		if err != nil {
			s.metrics.NackErrorsTotal.WithLabelValues(queue, metrics.ErrorBadgerDelete).Inc()
			return err
		}

		err = txn.Set([]byte(fmt.Sprintf("%s:%s:%s", scheduleQueueKey, marshalJob.Queue, jobId)), []byte{})
		if err != nil {
			s.metrics.NackErrorsTotal.WithLabelValues(queue, metrics.ErrorBadgerSet).Inc()
			return err
		}

		s.metrics.InflightJobs.WithLabelValues(queue, metrics.QueueStateInflight).Dec()
		s.metrics.ScheduleDepth.WithLabelValues(queue, metrics.QueueStateRetry).Inc()

		return nil
	})

	if err != nil {
		return isDlq, attemptCount, err
	}

	if isDlq {
		s.metrics.DLQTotal.WithLabelValues(queue).Inc()
	}

	s.metrics.NackTotal.WithLabelValues(queue, metrics.OperationNack).Inc()
	s.metrics.NackLatency.WithLabelValues(queue).Observe(time.Since(start).Seconds())

	return isDlq, attemptCount, nil
}

func (s *BadgerStore) Schedule(ctx context.Context, queue string, payload []byte, maxRetries int32, executeAt int64) (string, error) {
	return "", nil
}

func (s *BadgerStore) RequeueExpired() error {
	now := time.Now().Unix()
	getQueueKeyPrefix := []byte(fmt.Sprintf("%s:", inflightKeyQueueKey))
	getScheduledQueueKeyPrefix := []byte(fmt.Sprintf("%s:", scheduleQueueKey))

	err := s.db.Update(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(getQueueKeyPrefix); it.ValidForPrefix(getQueueKeyPrefix); it.Next() {
			key := it.Item().KeyCopy(nil)
			parts := strings.Split(string(key), ":")
			jobId := parts[len(parts)-1]
			leaseKey := []byte(fmt.Sprintf("%s:%s", leaseQueueKey, jobId))

			item, err := txn.Get(leaseKey)
			if err != nil {
				return err
			}

			var payload []byte
			err = item.Value(func(val []byte) error {
				payload = append([]byte{}, val...)
				return nil
			})
			if err != nil {
				return err
			}

			var lease *Lease
			err = json.Unmarshal(payload, &lease)
			if err != nil {
				return err
			}

			if lease.Expiry <= now {
				jobKey := []byte(fmt.Sprintf("%s:%s", jobQueueKey, jobId))

				item, err := txn.Get(jobKey)
				if err != nil {
					return err
				}

				var jobPayload []byte
				err = item.Value(func(val []byte) error {
					jobPayload = append([]byte{}, val...)
					return nil
				})
				if err != nil {
					return err
				}

				var job *Job
				err = json.Unmarshal(jobPayload, &job)
				if err != nil {
					return err
				}

				queue := job.Queue
				job.AttemptCount++

				if job.AttemptCount >= job.MaxRetries {
					job.Status = JobStatusFailed

					updatedJob, err := json.Marshal(job)
					if err != nil {
						return err
					}

					txn.Set([]byte(fmt.Sprintf("%s:%s", jobQueueKey, jobId)), []byte(updatedJob))
					txn.Delete([]byte(fmt.Sprintf("%s:%s", leaseQueueKey, jobId)))
					txn.Delete([]byte(fmt.Sprintf("%s:%s:%s", inflightKeyQueueKey, job.Queue, jobId)))
					txn.Set([]byte(fmt.Sprintf("%s:%s:%s", dlqQueueKey, job.Queue, jobId)), []byte{})

					s.metrics.LeaseExpiredTotal.WithLabelValues(queue).Inc()
					s.metrics.InflightJobs.WithLabelValues(queue, metrics.QueueStateInflight).Dec()
					s.metrics.DLQTotal.WithLabelValues(queue).Inc()
					s.metrics.DLQSize.WithLabelValues(queue).Inc()
				} else {
					job.Status = JobStatusQueued
					base := time.Now().Unix() + int64(1)
					job.RetryAfter = base

					updatedJob, err := json.Marshal(job)
					if err != nil {
						return err
					}

					txn.Set([]byte(fmt.Sprintf("%s:%s", jobQueueKey, jobId)), []byte(updatedJob))
					txn.Delete([]byte(fmt.Sprintf("%s:%s", leaseQueueKey, jobId)))
					txn.Delete([]byte(fmt.Sprintf("%s:%s:%s", inflightKeyQueueKey, job.Queue, jobId)))
					txn.Set([]byte(fmt.Sprintf("%s:%s:%s", scheduleQueueKey, job.Queue, jobId)), []byte{})

					s.metrics.LeaseExpiredTotal.WithLabelValues(queue).Inc()
					s.metrics.InflightJobs.WithLabelValues(queue, metrics.QueueStateInflight).Dec()
					s.metrics.RetryTotal.WithLabelValues(queue).Inc()
					s.metrics.ScheduleDepth.WithLabelValues(queue, metrics.QueueStateRetry).Inc()
				}
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	_ = s.db.Update(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(getScheduledQueueKeyPrefix); it.ValidForPrefix(getScheduledQueueKeyPrefix); it.Next() {
			key := it.Item().KeyCopy(nil)
			parts := strings.Split(string(key), ":")
			jobId := parts[len(parts)-1]
			scheduleJobKey := []byte(fmt.Sprintf("%s:%s", jobQueueKey, jobId))

			item, err := txn.Get(scheduleJobKey)
			if err != nil {
				return err
			}

			var payload []byte
			err = item.Value(func(val []byte) error {
				payload = append([]byte{}, val...)
				return nil
			})
			if err != nil {
				return err
			}

			var job *Job
			err = json.Unmarshal(payload, &job)
			if err != nil {
				return err
			}

			if job.RetryAfter <= now {
				queue := job.Queue
				job.Status = JobStatusQueued

				updatedJob, err := json.Marshal(job)
				if err != nil {
					return err
				}

				txn.Set([]byte(fmt.Sprintf("%s:%s", jobQueueKey, jobId)), []byte(updatedJob))
				txn.Set([]byte(fmt.Sprintf("%s:%s:%s", readyQueueKey, job.Queue, jobId)), []byte{})
				txn.Delete(key)

				s.metrics.ScheduleDepth.WithLabelValues(queue, metrics.QueueStateRetry).Dec()
				s.metrics.QueueDepth.WithLabelValues(queue, metrics.QueueStateReady).Inc()
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	return nil
}
