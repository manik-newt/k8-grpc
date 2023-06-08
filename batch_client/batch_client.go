// Sample implementation of a client container process that will
// use the batch processor.

package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	pb "github.com/maniknutanix/k8-grpc/batch_processor"
	"google.golang.org/grpc"
)

var (
	addr = flag.String("addr", "/tmp/batch_processor.sock", "path name of the unix socket")
)

// runNotificationLoop is a go routine that will run in the background and add/remove notification
// registration events.

func runNotificationLoop(c pb.BatchProcessorClient, requestIDChan chan string, responseChan chan *pb.ProgressResponseMsg, doneChan chan bool) {

	defer wg.Done()

	stream, err := c.NotifyProgressStream(context.Background())
	if err != nil {
		log.Fatalf("Unable to establish bi-directional notification stream")
	}

	// start the receiver stream
	go func(stream pb.BatchProcessor_NotifyProgressStreamClient, responseChan chan *pb.ProgressResponseMsg) {

		for {
			resp, err := stream.Recv()
			if err != nil {
				log.Printf("Stream disconnected: %v", err)
				return
			}
			if err == io.EOF {
				log.Printf("EOF received")
				return
			}

			log.Printf("Received notification: %v %v", resp.GetTaskProgress(), resp.GetStatusMsg())
			// Write the notification to the output channel
			responseChan <- resp
		}

	}(stream, responseChan)

	for {
		select {
		case <-doneChan:
			log.Printf("Done channel received signal. Exiting loop.")
			return
		case requestID := <-requestIDChan:
			log.Printf("Received request to subscribe notification for request id %v", requestID)
			err := stream.SendMsg(&pb.NotifyProgressRequestMsg{RequestId: requestID})
			if err != nil {
				log.Printf("Could not send request messge to server . Error %v", err)
			}
		}
	}
}

var wg sync.WaitGroup

func main() {
	flag.Parse()
	// Set up a connection to the server.
	// conn, err := grpc.Dial(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	conn, err := grpc.Dial("unix://"+*addr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("failed to dial: %v", err)
	}
	defer conn.Close()
	c := pb.NewBatchProcessorClient(conn)

	// Contact the server and print out its response.
	// Starting off with a test to see if the server is up and running
	// Send 100 or Create requests to the serrver

	requestIdArray := make([]string, 0)

	for i := 0; i < 10; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*120)
		defer cancel()
		rtype := pb.RequestType_SNAPSHOT
		if i%2 == 0 {
			rtype = pb.RequestType_CLONE
		}
		r, err := c.CreateRequest(ctx, &pb.CreateRequestMsg{Rtype: rtype,
			Name: fmt.Sprint("TestRequest-", i)})

		if err != nil {
			log.Fatalf("could not execute create request: %v", err)
		}

		requestIdArray = append(requestIdArray, r.GetCreateRequestId())
		log.Printf("Create: %s", r.GetCreateRequestId())
	}

	// Use the bidirectional stream to subscribe for notifications from the server.
	requestIDChan := make(chan string, 0)

	responseMsgChan := make(chan *pb.ProgressResponseMsg, 0)
	doneChan := make(chan bool)

	wg.Add(1)
	go runNotificationLoop(c, requestIDChan, responseMsgChan, doneChan)

	for _, requestID := range requestIdArray {
		// Subscribe for notifications for this request ID
		log.Printf("Sending requestID %v", requestID)
		requestIDChan <- requestID
	}

	taskCompletedCtr := 0

	for {
		select {
		case resp := <-responseMsgChan:
			//received a response for a registered notification
			if resp.TaskProgress == pb.ProgressResponseMsg_COMPLETED {
				// if this task was completed then we should unsubscribe for its
				// notification.
				// TODO : call the unubscribe RPC.
				taskCompletedCtr++
				msg, err := c.UnsubscribeProgress(context.Background(), &pb.UnsubscribeProgressMsg{RequestId: resp.GetRequestId()})
				if err != nil {
					log.Printf("Error unsubscribing for notification %v", msg.StatusMsg)
				} else {
					log.Printf("Unsubscribed for notification %v", resp.GetRequestId())
				}

				log.Printf("Total tasks completed %v/%v", taskCompletedCtr, len(requestIdArray))
			}
		}

		if taskCompletedCtr == len(requestIdArray) {
			log.Printf("Completed all pending tasks")
			close(doneChan)
			break
		}
	}

	wg.Wait()
}
