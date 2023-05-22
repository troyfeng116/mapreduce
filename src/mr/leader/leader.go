package leader

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"cs426.yale.edu/final-proj/src/clients"
	gfsProto "cs426.yale.edu/final-proj/src/gfs/proto"
	"cs426.yale.edu/final-proj/src/mr/leader/proto"
	workerProto "cs426.yale.edu/final-proj/src/mr/worker/proto"
	"cs426.yale.edu/final-proj/src/utils"
)

// ======== structs for tasks and statuses ========

type MapTask struct {
	taskId string
	inFile *utils.FileAndChunkId
}

type ReduceTask struct {
	taskId   string
	medFiles []*workerProto.MedFile
}

type TaskStatus int64

const (
	IDLE        TaskStatus = 0
	IN_PROGRESS            = 1
	COMPLETED              = 2
)

type TaskInfo struct {
	status                 TaskStatus
	nodeId                 *string
	numPartitionsCompleted int // for map tasks only
}

type ReduceTaskInfo struct {
	status     TaskStatus
	nodeId     *string
	outputFile *utils.FileAndChunkId // populated with output file once reduce task finishes
}

// tracks medFileId+medChunkId for workers that emit intermediate k-v pairs
type PendingReduceTasks struct {
	medFilesToReduce []*workerProto.MedFile
}

func (r *PendingReduceTasks) AddNewMedFile(medFile *workerProto.MedFile) {
	r.medFilesToReduce = append(r.medFilesToReduce, medFile)
}

// ======== structs for channel notifications ========

type TaskType = string

const (
	MAP_DONE      TaskType = "MAP_DONE"
	MAP_FAILED             = "MAP_FAILED"
	REDUCE_DONE            = "REDUCE_DONE"
	REDUCE_FAILED          = "REDUCE_FAILED"
)

type WorkerNotification struct {
	taskType TaskType
	taskId   string
	nodeId   string
	medFile  *workerProto.MedFile  // non-nil if MAP_DONE
	outFile  *utils.FileAndChunkId // non-nil if REDUCE_DONE
}

// ======== structs for worker statuses ========
// TODO: deprecate?
type WorkerStatus = int

const (
	WORKER_IDLE WorkerStatus = 0
	WORKER_BUSY WorkerStatus = 1
)

type LeaderServiceImpl struct {
	proto.UnimplementedLeaderServer

	mu         sync.RWMutex
	numNodes   int64
	nodeNames  []string
	R          int                     // number of reduce tasks
	filesToMap []*utils.FileAndChunkId // file+chunk ids of chunked input files to map

	// locally track task statuses
	mapTasks          map[*MapTask]*TaskInfo
	mapIdToTask       map[string]*MapTask
	isMapDone         bool
	reduceTaskTracker map[string]*PendingReduceTasks  // tracks intermediate k-v file locations while mapping in progress
	reduceTasks       map[*ReduceTask]*ReduceTaskInfo // once ready to reduce (i.e. once all maps done), will be set to non-nil
	reduceIdToTask    map[string]*ReduceTask
	reduceOutputs     map[string]*utils.FileAndChunkId // non-nil after reduce begins
	isReduceDone      bool

	// locally track worker statuses
	numJobsOnWorker map[string]int64

	// clients to communicate with GFS + workers
	gfsClient        gfsProto.GFSClient
	workerClientPool *clients.GrpcWorkerClientPool

	// target for final output write
	outFileId string

	workerCh chan WorkerNotification
}

func MakeLeaderServer(
	numNodes int64,
	nodeNames []string,
	R int,
	filesToMap []*utils.FileAndChunkId,
	gfsClient gfsProto.GFSClient,
	workerClientPool *clients.GrpcWorkerClientPool,
	outFileId string,
) *LeaderServiceImpl {
	mapTasks, mapIdToTask := initMapTasksAndIdToTasks(filesToMap)
	reduceTaskTracker := initReduceTaskTracker(R)

	leaderServer := &LeaderServiceImpl{
		numNodes:   numNodes,
		nodeNames:  nodeNames,
		R:          R,
		filesToMap: filesToMap,

		mapTasks:          mapTasks,
		mapIdToTask:       mapIdToTask,
		isMapDone:         false,
		reduceTaskTracker: reduceTaskTracker,
		reduceTasks:       nil, // populated after map tasks all complete
		reduceOutputs:     nil, // set after map tasks all complete
		reduceIdToTask:    nil, // set after map tasks all complete
		isReduceDone:      false,

		numJobsOnWorker: initNumJobsOnWorker(nodeNames),

		gfsClient:        gfsClient,
		workerClientPool: workerClientPool,
		outFileId:        outFileId,
		workerCh:         make(chan WorkerNotification),
	}
	// start long-running task monitor goroutine
	go leaderServer.monitorTasks()

	return leaderServer
}

// goroutine to monitor tasks statuses and delegate to worker nodes
func (leader *LeaderServiceImpl) monitorTasks() {
	ticker := time.NewTicker(1000 * time.Millisecond)
	for {
		leader.mu.Lock()
		select {
		case <-ticker.C:
			if !leader.isMapDone {
				// check worker statuses, assign new tasks if possible
				if leader.areAllMapTasksDone() {
					fmt.Println("[leader.monitorTasks()] all map tasks done!")
					leader.isMapDone = true
					// test: make sure all workers are idle (0 tasks)
					if !leader.areAllWorkersIdle() {
						fmt.Println("[leader.monitorTasks()] all map tasks done, but worker not idle?")
					}
					leader.initReduceFields() // create reduce tasks + output trackers
				} else {
					fmt.Println("[leader.monitorTasks()] assigning new map tasks...")
					leader.assignNewMapTasks()
				}
			} else if !leader.isReduceDone {
				if leader.areAllReduceTasksDone() {
					fmt.Println("[leader.monitorTasks()] all reduce tasks done!")
					leader.isReduceDone = true
					// test: make sure all workers are idle (0 tasks)
					if !leader.areAllWorkersIdle() {
						fmt.Println("[leader.monitorTasks()] all reduce tasks done, but worker not idle?")
					}
				} else {
					fmt.Println("[leader.monitorTasks()] assigning new reduce tasks...")
					leader.assignNewReduceTasks()
				}
			} else {
				// write reduced output k-v pairs to actual file
				go leader.writeFinalOutput()
				leader.mu.Unlock()
				fmt.Println("[leader.monitorTasks()] leader map+reduce done, ending goroutine")
				return
			}
			// default:
			// 	leader.mu.Unlock()
			// 	fmt.Println("[leader.monitorTasks()] hit default, ending goroutine")
			// 	return
		}

		leader.mu.Unlock()
	}
}

// called by workers to notify leader that a map task has been finished
func (leader *LeaderServiceImpl) NotifyMapTaskDone(ctx context.Context, req *proto.NotifyMapTaskDoneRequest) (*proto.NotifyMapTaskDoneResponse, error) {
	fmt.Printf(
		"[leader.NotifyMapTaskDone] node %s emitting intermediate file fileId=%s, chunkId=%s for mapTaskId=%s (reduceTaskId=%s)\n",
		req.ReqNodeId,
		req.MedFileId,
		req.MedChunkId,
		req.MapTaskId,
		req.ReduceTaskId,
	)
	leader.mu.Lock()
	defer leader.mu.Unlock()

	leader.numJobsOnWorker[req.ReqNodeId]--
	mapTask := leader.mapIdToTask[req.MapTaskId]
	leader.mapTasks[mapTask].nodeId = nil
	if leader.mapTasks[mapTask].status != COMPLETED {
		// TODO: do not complete parts more than once?
		leader.mapTasks[mapTask].numPartitionsCompleted++
		if leader.mapTasks[mapTask].numPartitionsCompleted == leader.R {
			leader.mapTasks[mapTask].status = COMPLETED
		}
		// new intermediate file emitted by worker -> update pending reduce tasks
		leader.reduceTaskTracker[req.ReduceTaskId].AddNewMedFile(&workerProto.MedFile{
			FileId:  req.MedFileId,
			ChunkId: req.MedChunkId,
		})
	}
	return &proto.NotifyMapTaskDoneResponse{}, nil
}

// called by workers to notify leader that a reduce task has been finished
func (leader *LeaderServiceImpl) NotifyReduceTaskDone(ctx context.Context, req *proto.NotifyReduceTaskDoneRequest) (*proto.NotifyReduceTaskDoneResponse, error) {
	fmt.Printf(
		"[leader.NotifyReduceTaskDone] node %s done reducing reduceTaskId %s, outFileId=%s, outputChunkId=%s\n",
		req.ReqNodeId,
		req.ReduceTaskId,
		req.OutFileId,
		req.OutChunkId,
	)
	leader.mu.Lock()
	defer leader.mu.Unlock()

	leader.numJobsOnWorker[req.ReqNodeId]--
	reduceTask := leader.reduceIdToTask[req.ReduceTaskId]
	leader.reduceTasks[reduceTask].nodeId = nil
	if leader.reduceTasks[reduceTask].status != COMPLETED {
		leader.reduceTasks[reduceTask].status = COMPLETED
		leader.reduceOutputs[req.ReduceTaskId] = &utils.FileAndChunkId{
			FileId:  req.OutFileId,
			ChunkId: req.OutChunkId,
		}
	}
	return &proto.NotifyReduceTaskDoneResponse{}, nil
}

// ======== utils ========

func initMapTasksAndIdToTasks(filesToMap []*utils.FileAndChunkId) (map[*MapTask]*TaskInfo, map[string]*MapTask) {
	mapTasks := make(map[*MapTask]*TaskInfo)
	mapIdToTask := make(map[string]*MapTask)
	for _, inFile := range filesToMap {
		mapTaskId := utils.GetMapTaskId(inFile.FileId, inFile.ChunkId)
		mapTask := &MapTask{
			taskId: mapTaskId,
			inFile: inFile,
		}
		mapTasks[mapTask] = &TaskInfo{
			status: IDLE,
			nodeId: nil,
		}

		mapIdToTask[mapTaskId] = mapTask
	}
	return mapTasks, mapIdToTask
}

func initReduceTaskTracker(R int) map[string]*PendingReduceTasks {
	reduceTaskTracker := make(map[string]*PendingReduceTasks)
	for reduceIdx := 0; reduceIdx < R; reduceIdx++ {
		reduceTaskId := utils.GetReduceTaskName(reduceIdx)
		reduceTaskTracker[reduceTaskId] = &PendingReduceTasks{
			medFilesToReduce: make([]*workerProto.MedFile, 0),
		}
	}
	return reduceTaskTracker
}

func initNumJobsOnWorker(nodeNames []string) map[string]int64 {
	numJobsOnWorker := make(map[string]int64, 0)
	for _, nodeName := range nodeNames {
		numJobsOnWorker[nodeName] = 0
	}
	return numJobsOnWorker
}

// assumes mutex lock
// should only be called after isMapDone set to true
func (leader *LeaderServiceImpl) initReduceFields() {
	leader.reduceTasks = make(map[*ReduceTask]*ReduceTaskInfo)
	leader.reduceOutputs = make(map[string]*utils.FileAndChunkId)
	leader.reduceIdToTask = make(map[string]*ReduceTask)
	for reduceTaskId, pendingReduceTasks := range leader.reduceTaskTracker {
		fmt.Printf("%s: %v\n", reduceTaskId, pendingReduceTasks)
		reduceTask := &ReduceTask{
			taskId:   reduceTaskId,
			medFiles: pendingReduceTasks.medFilesToReduce,
		}
		leader.reduceTasks[reduceTask] = &ReduceTaskInfo{
			status:     IDLE,
			nodeId:     nil,
			outputFile: nil,
		}

		leader.reduceIdToTask[reduceTaskId] = reduceTask

		leader.reduceOutputs[reduceTaskId] = nil
	}
}

// assumes mutex lock
func (leader *LeaderServiceImpl) areAllMapTasksDone() bool {
	for _, v := range leader.mapTasks {
		if v.status != COMPLETED {
			return false
		}
	}
	return true
}

// assumes mutex lock
func (leader *LeaderServiceImpl) areAllReduceTasksDone() bool {
	for _, v := range leader.reduceTasks {
		if v.status != COMPLETED {
			return false
		}
	}
	return true
}

// assumes mutex lock
func (leader *LeaderServiceImpl) areAllWorkersIdle() bool {
	for _, numTasks := range leader.numJobsOnWorker {
		if numTasks > 0 {
			return false
		}
	}
	return true
}

// ======== task assignment logic ========

// assumes mutex lock
func (leader *LeaderServiceImpl) getIdlestWorker() string {
	leastTasks, idlestWorker := int64(math.MaxInt64), ""
	for workerId, numTasks := range leader.numJobsOnWorker {
		if numTasks < leastTasks {
			leastTasks, idlestWorker = numTasks, workerId
		}
	}
	return idlestWorker
}

// assumes mutex lock
func (leader *LeaderServiceImpl) assignIdleMapTaskToWorker(idleMapTask *MapTask, workerId string) error {
	workerClient, err := leader.workerClientPool.GetWorkerClient(workerId)
	if err != nil {
		return err
	}

	leader.numJobsOnWorker[workerId]++
	leader.mapTasks[idleMapTask].nodeId = &workerId
	leader.mapTasks[idleMapTask].status = IN_PROGRESS
	// asynchronously assign task to worker via RPC
	go func() {
		_, err := workerClient.AssignMapTask(context.TODO(), &workerProto.AssignMapTaskRequest{
			MapTaskId: idleMapTask.taskId,
			InFileId:  idleMapTask.inFile.FileId,
			InChunkId: idleMapTask.inFile.ChunkId,
		})
		if err != nil {
			leader.mu.Lock()
			defer leader.mu.Unlock()
			leader.numJobsOnWorker[workerId]--
			leader.mapTasks[idleMapTask].nodeId = nil
			leader.mapTasks[idleMapTask].status = IDLE
		}
	}()

	return nil
}

// assumes mutex lock
// finds idlest worker, assigns map task to it
func (leader *LeaderServiceImpl) assignNewMapTasks() {
	idleMapTasks := make([]*MapTask, 0)
	for mapTask, mapTaskInfo := range leader.mapTasks {
		if mapTaskInfo.status == IDLE {
			idleMapTasks = append(idleMapTasks, mapTask)
		}
	}
	if len(idleMapTasks) == 0 {
		return
	}

	for _, idleMapTask := range idleMapTasks {
		idlestWorkerId := leader.getIdlestWorker()
		leader.assignIdleMapTaskToWorker(idleMapTask, idlestWorkerId)
	}
}

// assumes mutex lock
func (leader *LeaderServiceImpl) assignIdleReduceTaskToWorker(idleReduceTask *ReduceTask, workerId string) error {
	workerClient, err := leader.workerClientPool.GetWorkerClient(workerId)
	if err != nil {
		return err
	}

	leader.numJobsOnWorker[workerId]++
	leader.reduceTasks[idleReduceTask].nodeId = &workerId
	leader.reduceTasks[idleReduceTask].status = IN_PROGRESS
	// asynchronously assign task to worker via RPC
	go func() {
		// TODO: process RPC res
		_, err := workerClient.AssignReduceTask(context.TODO(), &workerProto.AssignReduceTaskRequest{
			ReduceTaskId:     idleReduceTask.taskId,
			MedFilesToReduce: idleReduceTask.medFiles,
		})
		if err != nil {
			leader.mu.Lock()
			defer leader.mu.Unlock()
			leader.numJobsOnWorker[workerId]--
			leader.reduceTasks[idleReduceTask].nodeId = nil
			leader.reduceTasks[idleReduceTask].status = IDLE
		}
	}()

	return nil
}

// assumes mutex lock
// finds idlest worker, assigns reduce task to it
func (leader *LeaderServiceImpl) assignNewReduceTasks() {
	idleReduceTasks := make([]*ReduceTask, 0)
	for reduceTask, reduceTaskInfo := range leader.reduceTasks {
		if reduceTaskInfo.status == IDLE {
			idleReduceTasks = append(idleReduceTasks, reduceTask)
		}
	}
	if len(idleReduceTasks) == 0 {
		return
	}

	for _, idleReduceTask := range idleReduceTasks {
		idlestWorkerId := leader.getIdlestWorker()
		leader.assignIdleReduceTaskToWorker(idleReduceTask, idlestWorkerId)
	}
}

func (leader *LeaderServiceImpl) writeFinalOutput() error {
	// test: make sure all reduce tasks done
	fmt.Println(leader.reduceOutputs)
	for reduceTaskId, outFile := range leader.reduceOutputs {
		readRes, err := leader.gfsClient.Read(context.TODO(), &gfsProto.ReadRequest{
			ReqNodeId: "leader",
			FileId:    outFile.FileId,
			ChunkId:   outFile.ChunkId,
		})
		if err != nil {
			fmt.Println(err)
			continue
		}

		fileChunk := readRes.FileChunk
		kvs, err := utils.ReadOutKvSplitFromOutFileChunk(fileChunk)
		if err != nil {
			fmt.Println(err)
			continue
		}

		utils.CreateAndWriteOutKvsToFinalOutFile(utils.GetFinalOutFileName(leader.outFileId, reduceTaskId), kvs)
	}

	return nil
}
