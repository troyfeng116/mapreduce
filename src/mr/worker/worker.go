package worker

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"cs426.yale.edu/final-proj/src/clients"
	gfsProto "cs426.yale.edu/final-proj/src/gfs/proto"
	leaderProto "cs426.yale.edu/final-proj/src/mr/leader/proto"
	"cs426.yale.edu/final-proj/src/mr/worker/proto"
	"cs426.yale.edu/final-proj/src/utils"
)

type WorkerServerImpl struct {
	proto.UnimplementedWorkerServer

	mu       sync.RWMutex
	nodeName string

	numNodes  int64
	nodeNames []string
	R         int // number of partitions for reduce tasks after map

	// clients to communicate with GFS + leader + workers
	gfsClient gfsProto.GFSClient
	// TODO: leader
	leaderClient     leaderProto.LeaderClient
	workerClientPool *clients.GrpcWorkerClientPool

	// all worker nodes have access to user-defined map+reduce functions
	mapf    func(filename string, contents string) []utils.KeyValue
	reducef func(key string, values []string) string
}

func MakeWorkerServer(
	nodeName string,
	numNodes int64,
	nodeNames []string,
	R int,
	gfsClient gfsProto.GFSClient,
	leaderClient leaderProto.LeaderClient,
	workerClientPool *clients.GrpcWorkerClientPool,
	mapf func(filename string, contents string) []utils.KeyValue,
	reducef func(key string, values []string) string,
) *WorkerServerImpl {
	return &WorkerServerImpl{
		nodeName:         nodeName,
		numNodes:         numNodes,
		nodeNames:        nodeNames,
		R:                R,
		gfsClient:        gfsClient,
		leaderClient:     leaderClient,
		workerClientPool: workerClientPool,
		mapf:             mapf,
		reducef:          reducef,
	}
}

func (worker *WorkerServerImpl) performMapTask(mapTaskId string, inFileId string, inChunkId string) error {
	// read input file chunk from GFS
	readRes, err := worker.gfsClient.Read(context.TODO(), &gfsProto.ReadRequest{
		ReqNodeId: worker.nodeName,
		FileId:    inFileId,
		ChunkId:   inChunkId,
	})
	if err != nil {
		return err
	}

	fileChunk := readRes.FileChunk
	// TODO: use inFileId+chunkId key here?
	// apply Map function
	medKvs := worker.mapf(inFileId, string(fileChunk.Contents))

	// partition intermediate kv pairs into R pieces for reduce
	medKvsSplit := utils.Partition(medKvs, worker.R)
	for reduceIdx, medKvSplit := range medKvsSplit {
		// write kvSplit to intermediate file, on this node's disk
		medFileChunk := utils.CreateMedFileChunkFromMedKvSplit(medKvSplit, inFileId, reduceIdx)
		_, err := worker.gfsClient.WriteToNode(context.TODO(), &gfsProto.WriteToNodeRequest{
			ReqNodeId: worker.nodeName,
			NodeId:    worker.nodeName,
			FileChunk: medFileChunk,
		})
		if err != nil {
			return err
		}

		// TODO: notify leader of new reduce task to be allocated
		go worker.leaderClient.NotifyMapTaskDone(context.TODO(), &leaderProto.NotifyMapTaskDoneRequest{
			ReqNodeId: worker.nodeName,
			MapTaskId: mapTaskId,
			// TODO: be careful with reduce task name
			ReduceTaskId: utils.GetReduceTaskName(reduceIdx),
			MedFileId:    medFileChunk.FileId,
			MedChunkId:   medFileChunk.ChunkId,
		})
	}

	return nil
}

// called by leader to assign map task to this worker
func (worker *WorkerServerImpl) AssignMapTask(ctx context.Context, req *proto.AssignMapTaskRequest) (*proto.AssignMapTaskResponse, error) {
	fmt.Printf("[worker.AssignMapTask] node %s assigned Map task for fileId=%s, chunkId=%s\n", worker.nodeName, req.InFileId, req.InChunkId)

	err := worker.performMapTask(req.MapTaskId, req.InFileId, req.InChunkId)
	if err != nil {
		return &proto.AssignMapTaskResponse{Success: false}, err
	}
	return &proto.AssignMapTaskResponse{
		Success: true,
	}, nil
}

func (worker *WorkerServerImpl) performReduceTask(medFiles []*proto.MedFile, reduceTaskId string) (outFileId string, outChunkId string, err error) {
	allMedKvs := make([]utils.KeyValue, 0)
	// read all intermediate files, collect all intermediate k-v pairs
	for _, medFile := range medFiles {
		// read input file chunk from GFS
		// TODO: read directly from worker peer via RPC
		readRes, err := worker.gfsClient.Read(context.TODO(), &gfsProto.ReadRequest{
			ReqNodeId: worker.nodeName,
			FileId:    medFile.FileId,
			ChunkId:   medFile.ChunkId,
		})
		if err != nil {
			return "", "", err
		}

		fileChunk := readRes.FileChunk
		kvSplit, err := utils.ReadMedKvSplitFromMedFileChunk(fileChunk)
		if err != nil {
			return "", "", err
		}

		allMedKvs = append(allMedKvs, kvSplit...)
	}

	// sort by intermediate keys
	sort.Sort(utils.SortableByKey(allMedKvs))

	// aggregate keys, apply reduce
	start := 0
	reducedKvs := make([]utils.KeyValue, 0)
	for start < len(allMedKvs) {
		end := start + 1
		for end < len(allMedKvs) && allMedKvs[start].Key == allMedKvs[end].Key {
			end++
		}
		aggValues := make([]string, 0)
		for i := start; i < end; i++ {
			aggValues = append(aggValues, allMedKvs[i].Value)
		}
		reduceOutput := worker.reducef(allMedKvs[start].Key, aggValues)
		reducedKvs = append(reducedKvs, utils.KeyValue{
			Key:   allMedKvs[start].Key,
			Value: reduceOutput,
		})
		start = end
	}

	// generate final output file for this reduce task
	outputFileId := reduceTaskId // TODO: make sure leader generates reduceTaskId
	outputChunkId := "0"         // TODO: chunkId for out file
	_, writeErr := worker.gfsClient.Write(context.TODO(), &gfsProto.WriteRequest{
		ReqNodeId: worker.nodeName,
		FileChunk: utils.CreateOutFileChunkFromReducedKvs(reducedKvs, outputFileId, outputChunkId),
	})

	if writeErr != nil {
		return "", "", writeErr
	}

	go worker.leaderClient.NotifyReduceTaskDone(context.TODO(), &leaderProto.NotifyReduceTaskDoneRequest{
		ReqNodeId:    worker.nodeName,
		ReduceTaskId: reduceTaskId,
		OutFileId:    outputFileId,
		OutChunkId:   outputChunkId,
	})
	return outputFileId, outChunkId, nil
}

// called by leader to assign reduce task to this worker
func (worker *WorkerServerImpl) AssignReduceTask(ctx context.Context, req *proto.AssignReduceTaskRequest) (*proto.AssignReduceTaskResponse, error) {
	fmt.Printf("[worker.AssignReduceTask] node %s assigned Reduce task for ReduceTaskId=%s, MedFiles=%v\n", worker.nodeName, req.ReduceTaskId, req.MedFilesToReduce)

	outFileId, outChunkId, err := worker.performReduceTask(req.MedFilesToReduce, req.ReduceTaskId)
	if err != nil {
		return &proto.AssignReduceTaskResponse{
			Success: false,
		}, err
	}
	return &proto.AssignReduceTaskResponse{
		Success:    true,
		OutFileId:  outFileId,
		OutChunkId: outChunkId,
	}, nil
}

// called by peer worker to retrieve intermediate file from this worker
func (worker *WorkerServerImpl) RequestMedFile(ctx context.Context, req *proto.RequestMedFileRequest) (*proto.RequestMedFileResponse, error) {
	return nil, nil
}
