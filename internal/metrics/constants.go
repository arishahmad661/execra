package metrics

const (
	QueueStateReady    = "ready"
	QueueStateInflight = "inflight"
	QueueStateRetry    = "retry"
	QueueStateDLQ      = "dlq"
)

const (
	OperationEnqueue = "enqueue"
	OperationDequeue = "dequeue"
	OperationAck     = "ack"
	OperationNack    = "nack"
	OperationRetry   = "retry"
	OperationClose   = "close"
)

const (
	ErrorMarshal           = "marshal_failed"
	ErrorUnmarshal         = "unmarshal_failed"
	ErrorBadgerUpdate      = "badger_update_failed"
	ErrorBadgerSet         = "badger_set_failed"
	ErrorBadgerGet         = "badger_get_failed"
	ErrorBadgerDelete      = "badger_delete_failed"
	ErrorBadgerClose       = "badger_close_failed"
	ErrorTransaction       = "transaction_failed"
	ErrorJobNotFound       = "job_not_found"
	ErrorLeaseMismatch     = "lease_mismatch"
	ErrorQueueNotFound     = "queue_not_found"
	ErrorLeaseExpired      = "lease_expired"
	ErrorInvalidJob        = "invalid_job"
	ErrorInvalidTransition = "invalid_transition"
)

const (
	RaftStatusSuccess = "success"
	RaftStatusFailure = "failure"
)
