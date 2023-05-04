// Package main implements a client for Greeter service.
package main

import (
	"context"
	"flag"
	"log"
	"time"

	pb "github.com/maniknutanix/k8-grpc/hello"
	"google.golang.org/grpc"
)

const (
	defaultName = "Rascala"
)

var (
	addr = flag.String("addr", "/tmp/greeter.sock", "path name of the unix socket")
	name = flag.String("name", defaultName, "Name to greet")
)

func main() {
	flag.Parse()
	// Set up a connection to the server.
	// conn, err := grpc.Dial(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	conn, err := grpc.Dial("unix://"+*addr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("failed to dial: %v", err)
	}
	defer conn.Close()
	c := pb.NewGreeterClient(conn)

	log.Printf("We are going to send a greeting every second, wokay ?")
	// Contact the server and print out its response.

	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		r, err := c.SayHello(ctx, &pb.HelloRequest{Name: *name})
		if err != nil {
			log.Fatalf("could not greet: %v", err)
		}
		cancel()
		log.Printf("Greeting: %s", r.GetMessage())
		time.Sleep(1 * time.Second)
	}

}
