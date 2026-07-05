package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	pb "github.com/arishahmad661/execra/gen/proto/execra/v1"
	"github.com/arishahmad661/execra/internal/api"
	"github.com/arishahmad661/execra/internal/metrics"
	"github.com/arishahmad661/execra/internal/store"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
)

func main() {
	port := "50051"
	prometheusPort := "9090"

	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()

	// store := store.NewMemoryStore()

	dir := os.TempDir()
	m := metrics.NewMetrics()

	store, err := store.NewBadgerDb(dir, m)
	server := api.NewServer(store)
	pb.RegisterQueueServiceServer(grpcServer, server)

	go func() {
		log.Printf("gRPC server running on :%s", port)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	go func() {
		log.Println(fmt.Sprintf("Metrics server running on :%s", prometheusPort))

		http.Handle("/metrics", promhttp.Handler())

		err := http.ListenAndServe(fmt.Sprintf(":%s", prometheusPort), nil)
		log.Fatalf("metrics server failed: %v", err)
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
