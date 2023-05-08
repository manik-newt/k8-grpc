// Package main implements a server for Greeter service.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"time"

	pb "github.com/maniknutanix/k8-grpc/hello"
	"google.golang.org/grpc"
)

var (
	addr = flag.String("addr", "/tmp/greeter.sock", "path name of the unix socket")
)

type clientStream struct {
	frequency uint32
	stream    pb.Greeter_ClientNotifyServer
	doneChan  chan bool
}

// server is used to implement helloworld.GreeterServer.
type server struct {
	pb.UnimplementedGreeterServer
	notifyStreams map[string]*clientStream
}

// SayHello implements helloworld.GreeterServer
func (s *server) SayHello(ctx context.Context, in *pb.HelloRequest) (*pb.HelloReply, error) {
	log.Printf("Received: %v", in.GetName())
	return &pb.HelloReply{Message: "What up dawg, Ahem Senor o Senora " + in.GetName()}, nil
}

func (s *server) ClientNotify(in *pb.HelloFrequency, stream pb.Greeter_ClientNotifyServer) error {

	log.Printf("Received: %v %v", in.GetFrequency(), in.GetClientId())
	frequency := in.GetFrequency()

	if frequency < 1 || frequency > 10 {
		log.Printf("Frequency %v is not in the range 1-10", frequency)
		return fmt.Errorf("Frequency %v is not in the range 1-10", frequency)
	}

	// If the clientID exists then return an error, otherwise add the clientID to the map
	doneChan := make(chan bool)
	if _, ok := s.notifyStreams[in.GetClientId()]; !ok {
		doneChan := make(chan bool)
		cs := &clientStream{frequency, stream, doneChan}
		s.notifyStreams[in.GetClientId()] = cs
	} else {
		return fmt.Errorf("Client ID %v already exists", in.GetClientId())
	}

	log.Printf("Client ID %v added to the map", in.GetClientId())
	<-doneChan
	return nil
}

func (s *server) clientNotifyLoop() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	log.Printf("Ticker started...")
	num_intervals := uint32(0)

	for {
		select {

		case <-ticker.C:
			num_intervals++
			for name, cs := range s.notifyStreams {
				if num_intervals%cs.frequency == 0 {
					if err := cs.stream.Send(&pb.HelloReply{Message: "What up dawg  " + name}); err != nil {
						log.Printf("Failed to send notification to client %v: %v", name, err)
						cs.doneChan <- true
						delete(s.notifyStreams, name) // remove the client's stream from the map
					}
				}
			}
		}
	}
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

	server := &server{notifyStreams: make(map[string]*clientStream)}

	pb.RegisterGreeterServer(s, server)
	go server.clientNotifyLoop()

	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
