// Package main implements a server for Greeter service.
package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"

	pb "github.com/maniknutanix/k8-grpc/hello"
	"google.golang.org/grpc"
)

var (
	addr = flag.String("addr", "/tmp/greeter.sock", "path name of the unix socket")
)

// server is used to implement helloworld.GreeterServer.
type server struct {
	pb.UnimplementedGreeterServer
}

// SayHello implements helloworld.GreeterServer
func (s *server) SayHello(ctx context.Context, in *pb.HelloRequest) (*pb.HelloReply, error) {
	log.Printf("Received: %v", in.GetName())
	return &pb.HelloReply{Message: "What up dawg, Ahem Senor o Senora " + in.GetName()}, nil
}

func main() {
	flag.Parse()

	if err := os.Remove(*addr); err != nil && !os.IsNotExist(err) {
		log.Fatalf("Failed to remove existing socket file: %v", err)
	}
	// lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	// Create the Unix socket listener
	lis, err := net.ListenUnix("unix", &net.UnixAddr{Name: *addr, Net: "unix"})
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterGreeterServer(s, &server{})
	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
