package api

import (
	"context"

	pb "github.com/arishahmad661/execra/gen/proto/execra/v1"
	"github.com/arishahmad661/execra/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	pb.UnimplementedQueueServiceServer
	store store.Store
}

func NewServer(s store.Store) *Server {
	return &Server{store: s}
}

func (s *Server) Enqueue(ctx context.Context, req *pb.EnqueueRequest) (*pb.EnqueueResponse, error) {
	if req.Queue == "" {
		return nil, status.Error(codes.InvalidArgument, "queue is required")
	}
	id, err := s.store.Enqueue(ctx, req.Queue, req.Payload, req.MaxRetries)
	if err != nil {
		return nil, err
	}

	return &pb.EnqueueResponse{JobId: id}, nil
}

func (s *Server) Dequeue(ctx context.Context, req *pb.DequeueRequest) (*pb.DequeueResponse, error) {
	if req.Queue == "" {
		return nil, status.Error(codes.InvalidArgument, "queue is required")
	}
	jobs, err := s.store.Dequeue(ctx, req.Queue, req.WorkerId, req.MaxJobs, req.LeaseDuration)
	if err != nil {
		return nil, err
	}

	var res []*pb.Job
	for _, j := range jobs {
		res = append(res, &pb.Job{
			JobId:   j.Job.Id,
			Payload: j.Job.Payload,
			LeaseId: j.Lease.LeaseID,
		})
	}

	return &pb.DequeueResponse{Jobs: res}, nil
}

func (s *Server) Ack(ctx context.Context, req *pb.AckRequest) (*pb.AckResponse, error) {
	err := s.store.Ack(ctx, req.JobId, req.LeaseId)
	if err != nil {
		return nil, err
	}
	return &pb.AckResponse{}, nil
}

func (s *Server) Nack(ctx context.Context, req *pb.NackRequest) (*pb.NackResponse, error) {
	isDlq, attempts, err := s.store.Nack(ctx, req.JobId, req.LeaseId, req.Error)
	if err != nil {
		return nil, err
	}

	return &pb.NackResponse{
		AttemptCount: attempts,
		IsDlq:        isDlq,
	}, nil
}

func (s *Server) Schedule(ctx context.Context, req *pb.ScheduleRequest) (*pb.ScheduleResponse, error) {
	if req.Queue == "" {
		return nil, status.Error(codes.InvalidArgument, "queue is required")
	}
	id, err := s.store.Schedule(ctx, req.Queue, req.Payload, req.MaxRetries, req.ExecuteAt)
	if err != nil {
		return nil, err
	}

	return &pb.ScheduleResponse{JobId: id}, nil
}

func (s *Server) Health(ctx context.Context, _ *pb.HealthRequest) (*pb.HealthResponse, error) {
	return &pb.HealthResponse{
		NodeId:   "node-0",
		IsLeader: true,
		Status:   "ok",
	}, nil
}
