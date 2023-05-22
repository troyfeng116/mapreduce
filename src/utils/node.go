package utils

import (
	"fmt"
)

/*
Node utility functions + constants.
*/

// @see adapted from Yale CS426 lab 4, shardmap.go
type NodeInfo struct {
	Address string `json:"address"`
	Port    int32  `json:"port"`
}

func NumNodesToNodeNames(numNodes int) []string {
	nodeNames := make([]string, 0)
	for i := 0; i < numNodes; i++ {
		nodeNames = append(nodeNames, fmt.Sprint(i))
	}
	return nodeNames
}

// takes nodeNames, address, and first worker's port number,
// returns map nodeName->NodeInfo
func NumNodesToNodeInfos(nodeNames []string, address string, startPort int) map[string]*NodeInfo {
	nodeInfos := make(map[string]*NodeInfo)
	for idx, nodeName := range nodeNames {
		nodeInfos[nodeName] = &NodeInfo{
			Address: address,
			Port:    int32(startPort + idx),
		}
	}
	return nodeInfos
}
