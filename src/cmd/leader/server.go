package main

import (
	"flag"
	"fmt"
	"net"

	"cs426.yale.edu/final-proj/src/clients"
	"cs426.yale.edu/final-proj/src/mr/leader"
	"cs426.yale.edu/final-proj/src/mr/leader/proto"
	"cs426.yale.edu/final-proj/src/utils"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

// @see adapted from cs426 lab 4: cmd/server/server.go

/*
main entrypoint to run leader server locally
*/

var (
	numWorkers = flag.Int64("num-workers", 0, "Number of worker nodes total")
	inputFiles = flag.String("input-files", "", "Regex to match input files")
	leaderAddr = flag.String("addr", "127.0.0.1", "Address on which to expose leader service")
	// TODO: read port from command line or json
	port        = flag.Int64("port", 8081, "Port number to expose for leader service")
	gfsAddr     = flag.String("gfs-addr", "127.0.0.1:8080", "Address (IP + port) of running GFS service")
	workersAddr = flag.String("worker-addr", "127.0.0.1", "Address of worker nodes") // TODO: json
	numReduce   = flag.Int64("num-reduce", 3, "Number of reduce tasks to distribute to workers")
	outFileId   = flag.String("out-file", "", "File path to write final reduced output files")
)

func main() {
	flag.Parse()

	// if *numWorkers <= 0 || len(*nodeName) == 0 || *port == 0 || len(*mrPlugin) == 0 {
	if *numWorkers <= 0 || len(*inputFiles) == 0 || len(*outFileId) == 0 {
		// logrus.Fatal("--num-workers (>=0), --node-name, --port, --mr-plugin required")
		logrus.Fatal("--num-workers (>=0), --input-files, --out-file required")
	}

	nodeNames := utils.NumNodesToNodeNames(int(*numWorkers))
	filePaths := utils.FindAllMatchingFilePaths(*inputFiles)
	allFileChunks := utils.FilePathsToChunks(filePaths)
	filesToMap := make([]*utils.FileAndChunkId, 0)
	for _, fileChunks := range allFileChunks {
		for _, fileChunk := range fileChunks {
			filesToMap = append(filesToMap, &utils.FileAndChunkId{
				FileId:  fileChunk.FileId,
				ChunkId: fileChunk.ChunkId,
			})
		}
	}

	// create worker client pool
	// TODO: pass this in via json to ensure all workers same? (rather than rely on calling same util)
	// TODO: how to determine start port?
	nodeInfos := utils.NumNodesToNodeInfos(nodeNames, *workersAddr, 8082)
	workerClientPool := clients.MakeWorkerClientPool(nodeInfos)

	gfsClient, err := clients.ConnectAndCreateGFSClient(*gfsAddr)
	if err != nil {
		logrus.Fatalf("failed to connect to GFS at addr %s: %v", gfsAddr, err)
	}

	server := grpc.NewServer()

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		logrus.Fatalf("failed to listen: %v", err)
	}

	proto.RegisterLeaderServer(
		server,
		leader.MakeLeaderServer(
			*numWorkers,
			nodeNames,
			int(*numReduce),
			filesToMap,
			gfsClient,
			&workerClientPool,
			*outFileId,
		),
	)
	logrus.Infof("leader server listening at %v", lis.Addr())
	if err := server.Serve(lis); err != nil {
		logrus.Fatalf("failed to serve: %v", err)
	}
}
