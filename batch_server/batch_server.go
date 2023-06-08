// This file implementes the batch server.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/google/uuid"
	"golang.design/x/lockfree"

	pb "github.com/maniknutanix/k8-grpc/batch_processor"
	"google.golang.org/grpc"
)

// Address of the UDS where the batch server will listen for job requests
var (
	addr                  = flag.String("addr", "/tmp/batch_processor.sock", "path name of the unix socket")
	createQueueInterval   = flag.Int("createQueueInterval", 1, "Interval in seconds for processing the create job queue")
	progressQueueInterval = flag.Int("progressQueueInterval", 10, "Interval             in seconds for processing the progress job queue")
	deleteQueueInterval   = flag.Int("deleteQueueInterval", 5, "Interval in seconds for processing the delete job queue")
)

type notifyProgressJob struct {
	// this tuple consists of clientStream and the NotifyProgressRequestMsg
	clientStream pb.BatchProcessor_NotifyProgressStreamServer

	// The notify progress request job.
	notifyProgressRequestMsg *pb.NotifyProgressRequestMsg
}

// This struct contains the queues for the batch server
type batchQueues struct {
	// Queue of jobs that need to be processed every second

	CreateJobQueue *lockfree.Queue

	// These jobs are processed every 30 seconds
	ProgressJobQueue      map[string]*notifyProgressJob
	ProgressJobQueueMutex sync.RWMutex // Mutex to protect the queue

	// These jobs are processed every 10 seconds
	DeleteJobQueue *lockfree.Queue

	// This channel is used to wake up the create job queue processing thread
	wakeupNotify chan bool
}

// CreateJobEntry struct that contains the client request proto along with the
// request id that is generated when we accept the create request.
type createJobEntry struct {
	requestId      string
	requestPayload *pb.CreateRequestMsg
}

// DeleteJobntry struct that contains the client request proto along with the
// request id that is generated when we accept the delete request.
type deleteJobEntry struct {
	deleteRequestId      string
	deleteRequestPayload *pb.DeleteRequestMsg
}

type batchServer struct {
	pb.UnimplementedBatchProcessorServer
	batchQueues *batchQueues
}

func newBatchQueues() *batchQueues {

	// Create and initalize a new batchQueue struct
	bq := &batchQueues{}

	bq.CreateJobQueue = lockfree.NewQueue()
	bq.ProgressJobQueue = make(map[string]*notifyProgressJob, 0)
	bq.DeleteJobQueue = lockfree.NewQueue()

	bq.wakeupNotify = make(chan bool, 1)

	return bq
}

func RequestTypeToString(rType pb.RequestType) string {
	switch rType {
	case pb.RequestType_SNAPSHOT:
		return "SNAPSHOT"
	case pb.RequestType_CLONE:
		return "CLONE"
	default:
		return "UNKNOWN"
	}
}

// Process the notifyProgress job queue.
func (bs *batchServer) processNotifyProgressQueue() {

	// whenever this function is called, we will cycle through each of the
	// entries in the progress queue.

	for {
		ok, _ := <-bs.batchQueues.wakeupNotify
		if !ok {
			// we have been asked to exit
			return
		}

		bs.batchQueues.ProgressJobQueueMutex.RLock()
		for requestID, job := range bs.batchQueues.ProgressJobQueue {
			bs.batchQueues.ProgressJobQueueMutex.RUnlock()

			// range through the list of jobs and invoke the API to get the
			// progress of the job that needs to be notified.
			log.Printf("Checking progress for job: %v", requestID)
			// TODO call the function here, that returns the notifify progress
			// for the job.
			response := TODO_notifyProgress(requestID, job)
			// Write the response to the client stream
			err := job.clientStream.Send(response)
			if err != nil {
				log.Printf("Failed to send notification to the client stream: %v", err)
				// remove this job from the progress queue since the stream may not be valid
				bs.batchQueues.ProgressJobQueueMutex.Lock()
				delete(bs.batchQueues.ProgressJobQueue, requestID)
				bs.batchQueues.ProgressJobQueueMutex.Unlock()
			}
			bs.batchQueues.ProgressJobQueueMutex.RLock()
		}
		bs.batchQueues.ProgressJobQueueMutex.RUnlock()
	}
}

func TODO_notifyProgress(requestID string, job *notifyProgressJob) *pb.ProgressResponseMsg {

	// TODO. Call the batchExecutor for each request type.
	log.Printf("Processing notify progress job: %v", job)

	// TODO. Once the requests have been queued  to the batch-executors the memory allocated for the
	// jobEntry can be returned to the pool.
	// Generate a random number between 0 and 3
	rand.Seed(time.Now().UnixNano())
	randomNum := rand.Intn(4)
	taskProgress := pb.ProgressResponseMsg_IN_PROGRESS
	status := "In progress"

	if randomNum == 0 {
		// 1 in 4 times, set TaskProgress to IN_COMPLETED
		taskProgress = pb.ProgressResponseMsg_COMPLETED
		status = "Finished"
	}
	// This should also be allocated from the pool
	return &pb.ProgressResponseMsg{
		RequestId:    requestID,
		TaskProgress: taskProgress,
		StatusMsg:    status,
	}

}

// This functon is called every createQueueInterval seconds to process the
// create job queue
func (bs *batchServer) processCreateJobQueue() {

	// We are using a lock-free queue so that we can dequeue the jobs entries
	// without having to acquire any locks. Once we dequeue the job entry, we
	// segregate the job entries based on the request type and call the batchExecutor

	if bs.batchQueues.CreateJobQueue.Length() == 0 {
		// nothing to process
		return
	}

	createJobBatch := make(map[pb.RequestType]*lockfree.Queue, 0)
	for {

		var jobEntry *createJobEntry
		var ok bool

		if jobEntry, ok = bs.batchQueues.CreateJobQueue.Dequeue().(*createJobEntry); !ok || jobEntry == nil {
			// We have processed all the jobs in the queue
			break
		}

		log.Printf("Processing create job entry: %v", jobEntry)

		// Check if we have already created a queue for this request type
		if _, ok := createJobBatch[jobEntry.requestPayload.Rtype]; !ok {
			createJobBatch[jobEntry.requestPayload.Rtype] = lockfree.NewQueue()
		}

		// Enqueue the job entry to the queue for this request type
		createJobBatch[jobEntry.requestPayload.Rtype].Enqueue(jobEntry)
	}

	// TODO. Call the batchExecutor for each request type.
	for rType, queue := range createJobBatch {
		log.Printf("Processing create job batch for request type: %v, %v", RequestTypeToString(rType), queue.Length())
	}

	// TODO. Once the requests have been queued  to the batch-executors the memory allocated for the
	// jobEntry can be returned to the pool.
	for _, queue := range createJobBatch {
		for {
			var jobEntry *createJobEntry
			var ok bool

			if jobEntry, ok = queue.Dequeue().(*createJobEntry); !ok || jobEntry == nil {
				// We have processed all the jobs in the queue
				break
			}
			CreateJobEntryPool.Put(jobEntry)
		}
	}
}

// Server side stubs for the batch server
func (bs *batchServer) CreateRequest(ctx context.Context, in *pb.CreateRequestMsg) (*pb.CreateResponseMsg, error) {
	log.Printf("Received CreateJob request: %v", in)

	// Generate a unique request ID for this job
	requestID := "Create_" + uuid.New().String() + in.Name + "_" + RequestTypeToString(in.Rtype)

	// Enqueue to the lock-free queue and return the request ID
	jobEntry := CreateJobEntryPool.Get().(*createJobEntry)

	jobEntry.requestId = requestID
	jobEntry.requestPayload = in

	bs.batchQueues.CreateJobQueue.Enqueue(jobEntry)

	return &pb.CreateResponseMsg{CreateRequestId: requestID}, nil
}

// Bidirectional stream implementation.
func (bs *batchServer) NotifyProgressStream(stream pb.BatchProcessor_NotifyProgressStreamServer) error {

	// schedule the receiver go-routine
	go func(stream pb.BatchProcessor_NotifyProgressStreamServer) {

	}(stream)

	ctx := stream.Context()
	for {
		// exit if context is done
		// or continue
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		in, err := stream.Recv()
		if err != nil {
			log.Printf("Client closed receiver stream")
			return err
		}

		log.Printf("About to queue a progress message")
		// Enqueue the request to the progress job queue
		np := NotifyProgressJobEntryPool.Get().(*notifyProgressJob)
		np.clientStream = stream
		np.notifyProgressRequestMsg = in

		bs.batchQueues.ProgressJobQueueMutex.Lock()
		if _, ok := bs.batchQueues.ProgressJobQueue[in.RequestId]; ok {
			// We already have an entry for this request id. Reject this request
			bs.batchQueues.ProgressJobQueueMutex.Unlock()
			log.Printf("Duplicate NotifyProgress request id: %v", in.RequestId)
		}
		bs.batchQueues.ProgressJobQueue[in.RequestId] = np
		bs.batchQueues.ProgressJobQueueMutex.Unlock()
	}
}

func (bs *batchServer) UnsubscribeProgress(ctx context.Context, in *pb.UnsubscribeProgressMsg) (*pb.UnsubscribeProgressMsg, error) {

	// Find the job entry for this request id in the notify progress map
	// and signal the close.
	respMsg := "Removed the request id from the progress queue"

	bs.batchQueues.ProgressJobQueueMutex.Lock()
	if _, ok := bs.batchQueues.ProgressJobQueue[in.RequestId]; ok {
		// Remove the entry from the map
		delete(bs.batchQueues.ProgressJobQueue, in.RequestId)
	} else {
		respMsg = fmt.Sprintf("Invalid request id: %v", in.RequestId)
	}

	bs.batchQueues.ProgressJobQueueMutex.Unlock()
	return &pb.UnsubscribeProgressMsg{StatusMsg: respMsg}, nil
}

// This function is similar to the processCreateJobQueue function. It is
// invoked by the batch server main loop every deleteJobQueuePollInterval and
// checks if there are any delete job entries in the delete job queue. If there
// are any delete job entries, it segregates them based on the request type and
// calls the batchExecutor for each request type.

func (bs *batchServer) processDeleteJobQueue() {

	if bs.batchQueues.DeleteJobQueue.Length() == 0 {
		// nothing to process
		return
	}

	deleteJobBatch := make(map[pb.RequestType]*lockfree.Queue, 0)

	for {
		var jobEntry *deleteJobEntry
		var ok bool

		if jobEntry, ok = bs.batchQueues.DeleteJobQueue.Dequeue().(*deleteJobEntry); !ok || jobEntry == nil {
			// We have processed all the jobs in the queue
			break
		}

		log.Printf("Processing create job entry: %v", jobEntry)

		// Check if we have already created a queue for this request type
		if _, ok := deleteJobBatch[jobEntry.deleteRequestPayload.Rtype]; !ok {
			deleteJobBatch[jobEntry.deleteRequestPayload.Rtype] = lockfree.NewQueue()
		}

		// Enqueue the job entry to the queue for this request type
		deleteJobBatch[jobEntry.deleteRequestPayload.Rtype].Enqueue(jobEntry)
	}

	// TODO. Call the batchExecutor for each request type.
	for rType, queue := range deleteJobBatch {
		log.Printf("Processing create job batch for request type: %v, %v", RequestTypeToString(rType), queue.Length())
	}

	// TODO. Once the requests have been queued  to the batch-executors the memory allocated for the
	// jobEntry can be returned to the pool.
	for _, queue := range deleteJobBatch {
		for {
			var jobEntry *deleteJobEntry
			var ok bool

			if jobEntry, ok = queue.Dequeue().(*deleteJobEntry); !ok || jobEntry == nil {
				// We have processed all the jobs in the queue
				break
			}
			deleteJobEntryPool.Put(jobEntry)
		}
	}
}

func (bs *batchServer) DeleteRequest(ctx context.Context, in *pb.DeleteRequestMsg) (*pb.DeleteResponseMsg, error) {

	// Find the job entry for this request id in the notify progress map
	// and signal the close.

	log.Printf("Received DeleteJob request: %v", in)

	// Generate a unique request ID for this job
	requestID := "Delete_" + uuid.New().String() + in.DeleteResourceName + "_" + RequestTypeToString(in.Rtype)

	// Enqueue to the lock-free queue and return the request ID
	jobEntry := deleteJobEntryPool.Get().(*deleteJobEntry)

	jobEntry.deleteRequestId = requestID
	jobEntry.deleteRequestPayload = in

	bs.batchQueues.DeleteJobQueue.Enqueue(jobEntry)

	return &pb.DeleteResponseMsg{DeleteRequestId: requestID}, nil
}

// Loop that runs endlessly looking for jobs to process
func (bs *batchServer) batchProcessorLoop() {

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	log.Printf("Ticker started...")
	num_intervals := uint64(0)

	for {
		select {

		case <-ticker.C:
			num_intervals++
			if num_intervals%uint64(*progressQueueInterval) == 0 {
				// send a wake up evene to the notify progress go routine
				bs.batchQueues.wakeupNotify <- true
			}
			if num_intervals%uint64(*deleteQueueInterval) == 0 {
				// Process the delete job queue
				bs.processDeleteJobQueue()
			}
			if num_intervals%uint64(*createQueueInterval) == 0 {
				// Process the create job queue
				bs.processCreateJobQueue()
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

	runtime.GOMAXPROCS(runtime.NumCPU() / 2)

	s := grpc.NewServer()

	server := &batchServer{batchQueues: newBatchQueues()}

	pb.RegisterBatchProcessorServer(s, server)
	go server.batchProcessorLoop()
	go server.processNotifyProgressQueue()

	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
