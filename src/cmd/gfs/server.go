package main

import (
	"flag"
	"fmt"
	"net"

	"cs426.yale.edu/final-proj/src/gfs"
	"cs426.yale.edu/final-proj/src/gfs/proto"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

// @see adapted from cs426 lab 4: cmd/server/server.go

/*
main entrypoint to run GFS server locally
*/

var (
	numWorkers = flag.Int64("num-workers", 0, "Number of worker nodes total")
	inputFiles = flag.String("input-files", "", "Regex to match input files")
	port       = flag.Int64("port", 8080, "Port number to expose for GFS service")
)

func main() {
	flag.Parse()

	if *numWorkers <= 0 || len(*inputFiles) == 0 {
		logrus.Fatal("--num-workers (>=0) and --input-files are required")
	}

	server := grpc.NewServer()

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		logrus.Fatalf("failed to listen: %v", err)
	}

	// clientPool := kv.MakeClientPool(&fileSm.ShardMap)

	proto.RegisterGFSServer(
		server,
		gfs.MakeGFSServer(*numWorkers, *inputFiles),
	)
	logrus.Infof("GFS server listening at %v", lis.Addr())
	if err := server.Serve(lis); err != nil {
		logrus.Fatalf("failed to serve: %v", err)
	}
}
