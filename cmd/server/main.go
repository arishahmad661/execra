package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	pb "github.com/arishahmad661/execra/gen/proto/execra/v1"
	"github.com/arishahmad661/execra/internal/api"
	"github.com/arishahmad661/execra/internal/store"
	"google.golang.org/grpc"
)

func main() {
	port := "50051"

	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	memStore := store.NewMemoryStore()
	server := api.NewServer(memStore)
	pb.RegisterQueueServiceServer(grpcServer, server)

	go func() {
		log.Printf("gRPC server running on :%s", port)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	<-stop
	log.Println("shutting down...")

	done := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		log.Println("forcing shutdown...")
		grpcServer.Stop()
	}
}
