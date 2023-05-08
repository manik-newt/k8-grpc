// Package main implements a client for Greeter service.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
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

func registerAndWatchForNotification(client pb.GreeterClient) {

	// Generate a unique client-ID
	clientID := "greeter-client-" + fmt.Sprintf("%d", time.Now().Unix())

	// register for a client notification every 5 seconds
	stream, err := client.ClientNotify(context.Background(), &pb.HelloFrequency{Frequency: 5, ClientId: clientID})
	if err != nil {
		log.Fatalf("Error registering for client notification: %v", err)
	}

	// Wait for the server to send a notification
	for {
		resp, err := stream.Recv() // This is a blocking call
		if err == io.EOF {
			log.Printf("EOF received")
			break
		} else if err != nil {
			log.Fatalf("Error receiving notification: %v", err)
		}

		log.Printf("Received notification: %v", resp.GetMessage())
	}

}

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

	// Contact the server and print out its response.

	// Starting off with a test to see if the server is up and running
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*120)
	defer cancel()
	r, err := c.SayHello(ctx, &pb.HelloRequest{Name: *name})
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}

	log.Printf("Greeting: %s", r.GetMessage())

	// Now register for a client notification and wait for the server to send a notification

	registerAndWatchForNotification(c)

}
