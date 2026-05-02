package main

import (
	"context"
	"fmt"
	"log"
	"time"

	pb "github.com/arishahmad661/execra/gen/proto/execra/v1"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var addr string

func main() {
	rootCmd := &cobra.Command{
		Use: "execra",
	}

	rootCmd.PersistentFlags().StringVar(&addr, "addr", "localhost:50051", "gRPC server address")

	rootCmd.AddCommand(enqueueCmd())
	rootCmd.AddCommand(dequeueCmd())
	rootCmd.AddCommand(ackCmd())
	rootCmd.AddCommand(nackCmd())
	rootCmd.AddCommand(healthCmd())

	if err := rootCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}

func getClient() (pb.QueueServiceClient, *grpc.ClientConn) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	return pb.NewQueueServiceClient(conn), conn
}

func enqueueCmd() *cobra.Command {
	var queue string
	var payload string
	var maxRetries int32

	cmd := &cobra.Command{
		Use: "enqueue",
		Run: func(cmd *cobra.Command, args []string) {
			client, conn := getClient()

			defer func() {
				if err := conn.Close(); err != nil {
					log.Println("failed to close connection:", err)
				}
			}()

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			res, err := client.Enqueue(ctx, &pb.EnqueueRequest{
				Queue:      queue,
				Payload:    []byte(payload),
				MaxRetries: maxRetries,
			})
			if err != nil {
				log.Fatal(err)
			}

			fmt.Println("Job ID:", res.JobId)
		},
	}

	cmd.Flags().StringVarP(&queue, "queue", "q", "", "Queue name")
	cmd.Flags().StringVarP(&payload, "payload", "p", "", "Job payload")
	cmd.Flags().Int32Var(&maxRetries, "retries", 3, "Max retries")

	if err := cmd.MarkFlagRequired("queue"); err != nil {
		log.Fatal(err)
	}
	if err := cmd.MarkFlagRequired("payload"); err != nil {
		log.Fatal(err)
	}

	return cmd
}

func dequeueCmd() *cobra.Command {
	var queue string
	var workerID string
	var maxJobs int32
	var leaseDuration int64

	cmd := &cobra.Command{
		Use: "dequeue",
		Run: func(cmd *cobra.Command, args []string) {
			client, conn := getClient()

			defer func() {
				if err := conn.Close(); err != nil {
					log.Println("failed to close connection:", err)
				}
			}()

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			res, err := client.Dequeue(ctx, &pb.DequeueRequest{
				Queue:         queue,
				WorkerId:      workerID,
				MaxJobs:       maxJobs,
				LeaseDuration: leaseDuration,
			})
			if err != nil {
				log.Fatal(err)
			}

			if len(res.Jobs) == 0 {
				fmt.Println("queue is empty")
				return
			}

			for _, j := range res.Jobs {
				fmt.Printf("Job: %s | Lease: %s | Payload: %s\n",
					j.JobId, j.LeaseId, string(j.Payload))
			}
		},
	}

	cmd.Flags().StringVarP(&queue, "queue", "q", "", "Queue name")
	cmd.Flags().StringVar(&workerID, "worker", "worker-1", "Worker ID")
	cmd.Flags().Int32Var(&maxJobs, "max-jobs", 1, "Max jobs to fetch")
	cmd.Flags().Int64Var(&leaseDuration, "lease", 30, "Lease duration (seconds)")

	if err := cmd.MarkFlagRequired("queue"); err != nil {
		log.Fatal(err)
	}
	return cmd
}

func ackCmd() *cobra.Command {
	return &cobra.Command{
		Use:  "ack [job_id] [lease_id]",
		Args: cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			client, conn := getClient()

			defer func() {
				if err := conn.Close(); err != nil {
					log.Println("failed to close connection:", err)
				}
			}()

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			_, err := client.Ack(ctx, &pb.AckRequest{
				JobId:   args[0],
				LeaseId: args[1],
			})
			if err != nil {
				log.Fatal(err)
			}

			fmt.Println("Acked")
		},
	}
}

func nackCmd() *cobra.Command {
	var errMsg string

	cmd := &cobra.Command{
		Use:  "nack [job_id] [lease_id]",
		Args: cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			client, conn := getClient()

			defer func() {
				if err := conn.Close(); err != nil {
					log.Println("failed to close connection:", err)
				}
			}()

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			res, err := client.Nack(ctx, &pb.NackRequest{
				JobId:   args[0],
				LeaseId: args[1],
				Error:   errMsg,
			})
			if err != nil {
				log.Fatal(err)
			}

			fmt.Printf("Attempts: %d | DLQ: %v\n", res.AttemptCount, res.IsDlq)
		},
	}

	cmd.Flags().StringVar(&errMsg, "error", "", "Error message")

	return cmd
}

func healthCmd() *cobra.Command {
	return &cobra.Command{
		Use: "health",
		Run: func(cmd *cobra.Command, args []string) {
			client, conn := getClient()

			defer func() {
				if err := conn.Close(); err != nil {
					log.Println("failed to close connection:", err)
				}
			}()

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			res, err := client.Health(ctx, &pb.HealthRequest{})
			if err != nil {
				log.Fatal(err)
			}

			fmt.Printf("Node: %s | Leader: %v | Status: %s\n",
				res.NodeId, res.IsLeader, res.Status)
		},
	}
}
