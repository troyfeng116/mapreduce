package clients

import (
	"cs426.yale.edu/final-proj/src/mr/leader/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// @see adapted from Yale CS426 lab 4: client_pool.go

// grpc.Dial leader node
func ConnectAndCreateLeaderClient(addr string) (proto.LeaderClient, error) {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))

	channel, err := grpc.Dial(addr, opts...)
	if err != nil {
		return nil, err
	}
	return proto.NewLeaderClient(channel), nil
}
