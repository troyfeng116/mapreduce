package main

import (
	"flag"
	"fmt"
	"net"

	"cs426.yale.edu/final-proj/src/clients"
	"cs426.yale.edu/final-proj/src/mr/worker"
	"cs426.yale.edu/final-proj/src/mr/worker/proto"
	"cs426.yale.edu/final-proj/src/utils"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

// @see adapted from cs426 lab 4: cmd/server/server.go

/*
main entrypoint to run worker server locally
*/

var (
	numWorkers = flag.Int64("num-workers", 0, "Number of worker nodes total")
	nodeName   = flag.String("node-name", "", "Name of the node")
	workerAddr = flag.String("addr", "127.0.0.1", "Address on which to expose worker service")
	// TODO: read port from command line or json
	// port           = flag.Int64("port", 0, "Port number to expose for worker service")
	mrPlugin   = flag.String("mr-plugin", "", "File path to plugin containing user-defined Map/Reduce function")
	gfsAddr    = flag.String("gfs-addr", "127.0.0.1:8080", "Address (IP + port) of running GFS service")
	leaderAddr = flag.String("leader-addr", "127.0.0.1:8081", "Address (IP + port) of running leader service")
	numReduce  = flag.Int64("num-reduce", 3, "Number of reduce tasks to distribute to workers")
)

func main() {
	flag.Parse()

	// if *numWorkers <= 0 || len(*nodeName) == 0 || *port == 0 || len(*mrPlugin) == 0 {
	if *numWorkers <= 0 || len(*nodeName) == 0 || len(*mrPlugin) == 0 {
		// logrus.Fatal("--num-workers (>=0), --node-name, --port, --mr-plugin required")
		logrus.Fatal("--num-workers (>=0), --node-name, --mr-plugin required")
	}

	nodeNames := utils.NumNodesToNodeNames(int(*numWorkers))

	gfsClient, err := clients.ConnectAndCreateGFSClient(*gfsAddr)
	if err != nil {
		logrus.Fatalf("failed to connect to GFS at addr %s: %v", gfsAddr, err)
	}

	leaderClient, err := clients.ConnectAndCreateLeaderClient(*leaderAddr)
	if err != nil {
		logrus.Fatalf("failed to connect to leader at addr %s: %v", leaderClient, err)
	}

	// create worker client pool
	// TODO: pass this in via json to ensure all workers same? (rather than rely on calling same util)
	// TODO: how to determine start port?
	nodeInfos := utils.NumNodesToNodeInfos(nodeNames, *workerAddr, 8082)
	workerClientPool := clients.MakeWorkerClientPool(nodeInfos)
	mapf, reducef := utils.LoadPlugin(*mrPlugin)

	server := grpc.NewServer()

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", nodeInfos[*nodeName].Port))
	if err != nil {
		logrus.Fatalf("failed to listen: %v", err)
	}

	proto.RegisterWorkerServer(
		server,
		worker.MakeWorkerServer(
			*nodeName,
			*numWorkers,
			utils.NumNodesToNodeNames(int(*numWorkers)),
			int(*numReduce),
			gfsClient,
			leaderClient,
			&workerClientPool,
			mapf,
			reducef,
		),
	)
	logrus.Infof("worker server %s listening at %v", *nodeName, lis.Addr())
	if err := server.Serve(lis); err != nil {
		logrus.Fatalf("failed to serve: %v", err)
	}

	// TODO: move this to tests for GFS
	// locs, err := gfsClient.GetLocs(context.TODO(), &proto.GetLocsRequest{ReqNodeId: "0", FileId: "src/files/pg-being_earnest.txt", ChunkId: "0"})
	// if err != nil {
	// 	logrus.Fatalf("unable to get locs from GFS: %v", err)
	// }
	// fmt.Println(locs.NodeIds)

	// _, err2 := gfsClient.Write(context.TODO(), &proto.WriteRequest{
	// 	ReqNodeId: "0",
	// 	FileChunk: &proto.FileChunk{
	// 		Contents: []byte("blah"),
	// 		FileId:   "test_file_id",
	// 		ChunkId:  "test_chunk_id",
	// 	},
	// })
	// if err2 != nil {
	// 	logrus.Fatalf("unable to write from GFS: %v", err2)
	// }

	// readRes, err3 := gfsClient.Read(context.TODO(), &proto.ReadRequest{
	// 	ReqNodeId: "0",
	// 	FileId:    "test_file_id",
	// 	ChunkId:   "test_chunk_id",
	// })
	// if err3 != nil {
	// 	logrus.Fatalf("unable to write from GFS: %v", err3)
	// }
	// fmt.Println(string(readRes.FileChunk.Contents))
}
