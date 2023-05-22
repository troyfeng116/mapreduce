package clients

import (
	"fmt"
	"sync"

	"cs426.yale.edu/final-proj/src/mr/worker/proto"
	"cs426.yale.edu/final-proj/src/utils"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// @see adapted from Yale CS426 lab 4: client_pool.go

type WorkerClientPool interface {
	GetWorkerClient(nodeName string) (*proto.WorkerClient, error)
}

// GrpcWorkerClientPool implements WorkerClientPool interface
type GrpcWorkerClientPool struct {
	mutex sync.RWMutex

	clients   map[string]proto.WorkerClient
	nodeInfos map[string]*utils.NodeInfo
}

// grpc.Dial worker node
func ConnectAndCreateWorkerClient(addr string) (proto.WorkerClient, error) {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))

	channel, err := grpc.Dial(addr, opts...)
	if err != nil {
		return nil, err
	}
	return proto.NewWorkerClient(channel), nil
}

// creates GrpcWorkerClientPool struct (implements WorkerClientPool)
func MakeWorkerClientPool(nodeInfos map[string]*utils.NodeInfo) GrpcWorkerClientPool {
	return GrpcWorkerClientPool{
		clients:   make(map[string]proto.WorkerClient),
		nodeInfos: nodeInfos,
	}
}

func (pool *GrpcWorkerClientPool) GetWorkerClient(nodeName string) (proto.WorkerClient, error) {
	// Optimistic read -- most cases we will have already cached the client, so
	// only take a read lock to maximize concurrency here
	pool.mutex.RLock()
	client, ok := pool.clients[nodeName]
	pool.mutex.RUnlock()
	if ok {
		return client, nil
	}

	pool.mutex.Lock()
	defer pool.mutex.Unlock()
	// We may have lost a race and someone already created a client, try again
	// while holding the exclusive lock
	client, ok = pool.clients[nodeName]
	if ok {
		return client, nil
	}

	nodeInfo, ok := pool.nodeInfos[nodeName]
	if !ok {
		logrus.WithField("node", nodeName).Errorf("unknown nodename passed to GetClient")
		return nil, fmt.Errorf("no node named: %s", nodeName)
	}

	// Otherwise create the client: gRPC expects an address of the form "ip:port"
	address := fmt.Sprintf("%s:%d", nodeInfo.Address, nodeInfo.Port)
	client, err := ConnectAndCreateWorkerClient(address)
	if err != nil {
		logrus.WithField("node", nodeName).Debugf("failed to connect to node %s (%s): %q", nodeName, address, err)
		return nil, err
	}
	pool.clients[nodeName] = client
	return client, nil
}
