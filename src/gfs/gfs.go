package gfs

/*
Implements GFS server logic, to be accessed from machines via RPCs.
*/

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sync"

	"cs426.yale.edu/final-proj/src/gfs/proto"
	"cs426.yale.edu/final-proj/src/utils"
)

// type GFSFileChunk struct {
// 	proto.FileChunk
// }

// simulated in-memory node with "file chunks" stored on its disk
type GFSInMemNode struct {
	nodeName string
	files    map[string]*proto.FileChunk // maps chunk hashes to chunk contents
}

func (gfsNode *GFSInMemNode) readFileChunk(fileId string, chunkId string) (*proto.FileChunk, error) {
	key := utils.FileAndChunkIdToKey(fileId, chunkId)
	fileChunk, ok := gfsNode.files[key]
	if fileChunk == nil || !ok {
		return nil, fmt.Errorf("[GFS.Read] node %s unable to find key %s for fileId=%s, chunkId=%s", gfsNode.nodeName, key, fileId, chunkId)
	}
	return fileChunk, nil
}

func (gfsNode *GFSInMemNode) writeFileChunk(fileChunk *proto.FileChunk) error {
	key := utils.FileAndChunkIdToKey(fileChunk.FileId, fileChunk.ChunkId)
	gfsNode.files[key] = fileChunk
	return nil
}

type GFSInMemFileSystem struct {
	chunkToNodeIds map[string][]string      // maps chunkKey to nodeIds containing chunk in disk
	nodeIdToNode   map[string]*GFSInMemNode // maps nodeIds to "nodes"
}

func (gfsFileSys *GFSInMemFileSystem) ReadFileChunk(nodeId string, fileId string, chunkId string) (*proto.FileChunk, error) {
	node, ok := gfsFileSys.nodeIdToNode[nodeId]
	if node == nil || !ok {
		return nil, fmt.Errorf("[GFS.Read] requested read target node %s not found", nodeId)
	}

	fileChunk, err := node.readFileChunk(fileId, chunkId)
	if err != nil {
		return nil, err
	}

	return fileChunk, nil
}

// write FileChunk to node, update chunkToNodeIds
func (gfsFileSys *GFSInMemFileSystem) WriteFileChunk(nodeId string, fileChunk *proto.FileChunk) error {
	node, ok := gfsFileSys.nodeIdToNode[nodeId]
	if node == nil || !ok {
		return fmt.Errorf("[GFS.Write] requested write target node %s not found", nodeId)
	}

	// update chunkToNodeIds
	key := utils.FileAndChunkIdToKey(fileChunk.FileId, fileChunk.ChunkId)
	nodeIdsBefore := gfsFileSys.chunkToNodeIds[key]
	if nodeIdsBefore == nil {
		gfsFileSys.chunkToNodeIds[key] = make([]string, 0)
	}
	gfsFileSys.chunkToNodeIds[key] = append(gfsFileSys.chunkToNodeIds[key], nodeId)

	return node.writeFileChunk(fileChunk)
}

// distribute file chunk across 3 randomly chosen unique nodes
func (gfsFileSys *GFSInMemFileSystem) DistributeFileChunk(
	fileChunk *proto.FileChunk,
	replicas int64,
	numNodes int64,
	nodeNames []string,
) error {
	n := int(math.Min(float64(replicas), math.Max(float64(numNodes)/2.0, 1)))
	used := make([]bool, numNodes)
	written := 0
	for written < n {
		nodeIdx := rand.Intn(int(numNodes))
		if used[nodeIdx] {
			continue
		}
		err := gfsFileSys.WriteFileChunk(nodeNames[nodeIdx], fileChunk)
		if err != nil {
			return err
		}
		used[nodeIdx] = true
		written++
	}
	return nil
}

type GFSServerImpl struct {
	proto.UnimplementedGFSServer

	mu           sync.RWMutex
	numNodes     int64
	nodeNames    []string
	inMemFileSys *GFSInMemFileSystem
}

// given input files, distribute file chunks across distributed file system
func MakeGFSServer(numNodes int64, inputFilesRegex string) *GFSServerImpl {
	nodeNames := utils.NumNodesToNodeNames(int(numNodes))
	filePaths := utils.FindAllMatchingFilePaths(inputFilesRegex)

	inMemFileSys := &GFSInMemFileSystem{
		chunkToNodeIds: make(map[string][]string),
		nodeIdToNode:   make(map[string]*GFSInMemNode),
	}
	for _, nodeName := range nodeNames {
		inMemFileSys.nodeIdToNode[nodeName] = &GFSInMemNode{
			nodeName: nodeName,
			files:    make(map[string]*proto.FileChunk),
		}
	}

	// initial placement of chunks: distribute randomly
	for _, p := range filePaths {
		// fmt.Println(p)
		fileChunks, err := utils.ParseFileIntoFileChunks(p, utils.CHUNK_SIZE)
		if err != nil {
			fmt.Printf("[GFS.MakeGFSServer] error reading file %s: %s", p, err.Error())
			return nil
		}

		for _, fileChunk := range fileChunks {
			// create 3 replicas of fileChunk at initialization
			err := inMemFileSys.DistributeFileChunk(fileChunk, 3, numNodes, nodeNames)
			if err != nil {
				return nil
			}
		}
	}

	gfsServer := &GFSServerImpl{
		numNodes:     numNodes,
		nodeNames:    nodeNames,
		inMemFileSys: inMemFileSys,
	}
	gfsServer.Dump()
	return gfsServer
}

// returns list of node names hosting file chunk
func (gfs *GFSServerImpl) GetLocs(ctx context.Context, req *proto.GetLocsRequest) (*proto.GetLocsResponse, error) {
	fmt.Printf("[GFS.GetLocs] node %s requesting locations for FileChunk with fileId=%s, chunkId=%s\n", req.ReqNodeId, req.FileId, req.ChunkId)

	gfs.mu.Lock()
	defer gfs.mu.Unlock()

	key := utils.FileAndChunkIdToKey(req.FileId, req.ChunkId)
	nodeIds, ok := gfs.inMemFileSys.chunkToNodeIds[key]
	if !ok {
		return &proto.GetLocsResponse{NodeIds: nil}, fmt.Errorf("[GFS.GetLocs] chunk key %s for fileId=%s, chunkId=%s not found", key, req.FileId, req.ChunkId)
	}

	return &proto.GetLocsResponse{NodeIds: nodeIds}, nil
}

// returns file chunk hosted on any node in file system
func (gfs *GFSServerImpl) Read(ctx context.Context, req *proto.ReadRequest) (*proto.ReadResponse, error) {
	fmt.Printf("[GFS.Read] node %s requesting to read FileChunk with fileId=%s, chunkId=%s from any node\n", req.ReqNodeId, req.FileId, req.ChunkId)

	gfs.mu.Lock()
	defer gfs.mu.Unlock()

	key := utils.FileAndChunkIdToKey(req.FileId, req.ChunkId)
	nodeIds, ok := gfs.inMemFileSys.chunkToNodeIds[key]
	if !ok || len(nodeIds) == 0 {
		return &proto.ReadResponse{FileChunk: nil}, fmt.Errorf("[GFS.Read] chunk key %s for fileId=%s, chunkId=%s not found", key, req.FileId, req.ChunkId)
	}

	fileChunk, err := gfs.inMemFileSys.ReadFileChunk(nodeIds[0], req.FileId, req.ChunkId)
	if err != nil {
		return nil, err
	}

	return &proto.ReadResponse{FileChunk: fileChunk}, nil
}

// returns file chunk hosted on specified node
func (gfs *GFSServerImpl) ReadFromNode(ctx context.Context, req *proto.ReadFromNodeRequest) (*proto.ReadFromNodeResponse, error) {
	fmt.Printf("[GFS.ReadFromNode] node %s requesting to read FileChunk with fileId=%s, chunkId=%s from node %s\n", req.ReqNodeId, req.FileId, req.ChunkId, req.NodeId)

	gfs.mu.Lock()
	defer gfs.mu.Unlock()

	fileChunk, err := gfs.inMemFileSys.ReadFileChunk(req.NodeId, req.FileId, req.ChunkId)
	if err != nil {
		return nil, err
	}

	return &proto.ReadFromNodeResponse{FileChunk: fileChunk}, nil
}

// writes file chunk to specified node
func (gfs *GFSServerImpl) WriteToNode(ctx context.Context, req *proto.WriteToNodeRequest) (*proto.WriteToNodeResponse, error) {
	fmt.Printf("[GFS.WriteToNode] node %s requesting to write FileChunk %v to node %s\n", req.ReqNodeId, truncateFileChunk(req.FileChunk), req.NodeId)

	gfs.mu.Lock()
	defer gfs.mu.Unlock()

	err := gfs.inMemFileSys.WriteFileChunk(req.NodeId, req.FileChunk)
	return &proto.WriteToNodeResponse{}, err
}

// writes file chunk to any three nodes
func (gfs *GFSServerImpl) Write(ctx context.Context, req *proto.WriteRequest) (*proto.WriteResponse, error) {
	fmt.Printf("[GFS.Write] node %s requesting to write FileChunk %v to any distributed nodes\n", req.ReqNodeId, truncateFileChunk(req.FileChunk))

	gfs.mu.Lock()
	defer gfs.mu.Unlock()

	// distribute new file chunk amongst max(3,numNodes) nodes in file system
	err := gfs.inMemFileSys.DistributeFileChunk(req.FileChunk, 3, gfs.numNodes, gfs.nodeNames)
	if err != nil {
		return &proto.WriteResponse{}, err
	}

	return &proto.WriteResponse{}, nil
}

// deletes specified file from file system
func (gfs *GFSServerImpl) Delete(ctx context.Context, req *proto.DeleteRequest) (*proto.DeleteResponse, error) {
	// TODO
	return nil, nil
}

func (gfs *GFSServerImpl) Dump() {
	fmt.Printf("numNodes=%d, nodeNames=%v\n", gfs.numNodes, gfs.nodeNames)
	gfs.mu.Lock()
	defer gfs.mu.Unlock()

	fmt.Println("======== gfs.inMemFileSys ========")
	fmt.Printf("chunkToNodeIds=%v\n", gfs.inMemFileSys.chunkToNodeIds)
	fmt.Printf("nodeIdToNode=%v\n", gfs.inMemFileSys.nodeIdToNode)
	fmt.Printf("================\n\n")
}

// ======== utils ========

func truncateFileChunk(fileChunk *proto.FileChunk) *proto.FileChunk {
	return &proto.FileChunk{
		FileId:   fileChunk.FileId,
		ChunkId:  fileChunk.ChunkId,
		Contents: fileChunk.Contents[:int(math.Min(float64(len(fileChunk.Contents)), 64))],
	}
}
