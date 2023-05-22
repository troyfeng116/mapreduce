package utils

import (
	"fmt"
	"hash/fnv"
	"math"
	"os"
	"path/filepath"
	"plugin"
	"strconv"
	"strings"

	gfsProto "cs426.yale.edu/final-proj/src/gfs/proto"
	"github.com/sirupsen/logrus"
)

/*
General utility functions + constants.
*/

type KeyValue struct {
	Key   string
	Value string
}

// implements sort.Interface: Len(), Less(), Swap()
type SortableByKey []KeyValue

func (e SortableByKey) Len() int           { return len(e) }
func (e SortableByKey) Less(i, j int) bool { return e[i].Key < e[j].Key }
func (e SortableByKey) Swap(i, j int)      { e[i], e[j] = e[j], e[i] }

// @see adapted from MIT 6.824 Lab 1: https://github.com/nsiregar/mit-go/blob/master/src/main/mrsequential.go
//
// load the application Map and Reduce functions
// from a plugin file, e.g. ../mrapps/wc.so
func LoadPlugin(filename string) (func(string, string) []KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		fmt.Println(err)
		logrus.Fatalf("cannot load plugin %v", filename)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		logrus.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		logrus.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}

// ======== os file helpers ========

const CHUNK_SIZE = math.MaxUint64

func FindAllMatchingFilePaths(pathRegex string) []string {
	fmt.Println(pathRegex)
	matches, _ := filepath.Glob(pathRegex)
	// fmt.Println(matches)
	for _, p := range matches {
		fmt.Println(p)
	}
	return matches
}

// TODO: for now, keep entire file in one chunk
func ParseFileIntoFileChunks(filePath string, chunkSize uint64) ([]*gfsProto.FileChunk, error) {
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	fileInfo, err := file.Stat()
	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	fileSize := fileInfo.Size()
	buffer := make([]byte, fileSize)

	bytesRead, err := file.Read(buffer)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	// s := string(buffer)
	fmt.Println("bytes read: ", bytesRead)
	// fmt.Println("bytestream to string: ", string(s))

	fileChunks := make([]*gfsProto.FileChunk, 1)
	// idx := 0
	// chunkIdx := 0
	// for idx < bytesRead {
	// 	end := uint64(math.Max(float64(idx + int(chunkSize)), float64(idx)))
	// 	fileChunks = append(fileChunks, &gfsProto.FileChunk{
	// 		Contents: buffer[idx : idx+int(chunkSize)],
	// 		FileId:   filePath, // TODO: better file ID?
	// 		ChunkId:  fmt.Sprint(chunkIdx),
	// 	})
	// 	idx += int(chunkSize)
	// 	chunkIdx++
	// }
	fileChunks[0] = &gfsProto.FileChunk{
		Contents: buffer,
		FileId:   filePath,
		ChunkId:  "0",
	}

	return fileChunks, nil
}

func FilePathsToChunks(filePaths []string) [][]*gfsProto.FileChunk {
	allFileChunks := make([][]*gfsProto.FileChunk, len(filePaths))
	for _, p := range filePaths {
		// fmt.Println(p)
		fileChunks, err := ParseFileIntoFileChunks(p, CHUNK_SIZE)
		if err != nil {
			fmt.Printf("[GFS.MakeGFSServer] error reading file %s: %s", p, err.Error())
			return nil
		}
		allFileChunks = append(allFileChunks, fileChunks)
	}
	return allFileChunks
}

func GetFinalOutFileName(outFileId string, reduceTaskId string) string {
	return fmt.Sprintf("out/mr-out_%s_%s.out", outFileId, reduceTaskId)
}

// helper to create file
func CreateAndWriteOutKvsToFinalOutFile(fileName string, kvs []KeyValue) error {
	file, err := os.Create(fileName)
	if err != nil {
		return err
	}

	// write all output k-v pairs to output file
	for _, kv := range kvs {
		s := fmt.Sprintf("%s: %s\n", kv.Key, kv.Value)
		_, err := file.Write([]byte(s))
		if err != nil {
			return err
		}
	}

	// fmt.Printf("[CreateAndWriteToFile] wrote %d bytes\n", n)
	return nil
}

// @see taken from Yale CS426 Lab 4, kv/util.go, GetShardForKey()
func Hash(key string, n int) int {
	hasher := fnv.New32()
	hasher.Write([]byte(key))
	return int(hasher.Sum32()) % n
}

// partitions intermediate k-v pairs into R partitions for reduce tasks
func Partition(kvs []KeyValue, R int) [][]KeyValue {
	splitKvs := make([][]KeyValue, R)
	for i := 0; i < R; i++ {
		splitKvs[i] = make([]KeyValue, 0)
	}

	h := 0
	for _, kv := range kvs {
		// h := Hash(kv.Key, R)
		// TODO: split according to value as well? split round robin?
		splitKvs[h%R] = append(splitKvs[h%R], kv)
		h++
	}

	return splitKvs
}

// ======== intermediate file handling logic ========

const SEP_ARR = "*$*"
const SEP_KV = "*/*"

type FileAndChunkId struct {
	FileId  string
	ChunkId string
}

func kvArrToString(kvArr []KeyValue) string {
	strKvArr := make([]string, 0)
	for i := 0; i < len(kvArr); i++ {
		strKvArr = append(strKvArr, fmt.Sprintf("%s%s%s", kvArr[i].Key, SEP_KV, kvArr[i].Value))
	}

	str := strings.Join(strKvArr, SEP_ARR)
	return str
}

// generate FileChunk from k-v array
func CreateMedFileChunkFromMedKvSplit(medKvSplit []KeyValue, inputFileId string, reduceIdx int) *gfsProto.FileChunk {
	return &gfsProto.FileChunk{
		Contents: []byte(kvArrToString(medKvSplit)),
		FileId:   fmt.Sprintf(GetIntermediateFileName(inputFileId, reduceIdx)),
		ChunkId:  "0", // TODO: how to chunk intermediate kv pairs?
	}
}

// generate FileChunk from k-v array corresponding to reduced intermediate k-v pairs
func CreateOutFileChunkFromReducedKvs(reducedKvs []KeyValue, outFileId string, outChunkId string) *gfsProto.FileChunk {
	return &gfsProto.FileChunk{
		Contents: []byte(kvArrToString(reducedKvs)),
		FileId:   outFileId,
		ChunkId:  outChunkId, // TODO: how to chunk intermediate kv pairs?
	}
}

func stringToMedKvSplit(str string) ([]KeyValue, error) {
	kvSplit := make([]KeyValue, 0)
	toks := strings.Split(str, SEP_ARR)
	for _, t := range toks {
		t_toks := strings.Split(t, SEP_KV)
		if len(t_toks) != 2 {
			return nil, fmt.Errorf("[utils.stringToMedKvSplit] unable to parse str %s into kv arr", str)
		}
		kvSplit = append(kvSplit, KeyValue{Key: t_toks[0], Value: t_toks[1]})
	}
	return kvSplit, nil
}

// read k-v array from intermediate FileChunk
func ReadMedKvSplitFromMedFileChunk(medFileChunk *gfsProto.FileChunk) ([]KeyValue, error) {
	return stringToMedKvSplit(string(medFileChunk.Contents))
}

// read k-v array from output FileChunk
func ReadOutKvSplitFromOutFileChunk(outFileChunk *gfsProto.FileChunk) ([]KeyValue, error) {
	return stringToMedKvSplit(string(outFileChunk.Contents))
}

func ReadFinalOutKvsFromAggUnreducedOutFile(outFile string) ([]KeyValue, error) {
	chunks, err := ParseFileIntoFileChunks(outFile, CHUNK_SIZE)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	kvs := make([]KeyValue, 0)
	for _, ch := range chunks {
		kvStrs := strings.Split(string(ch.Contents), "\n")
		for _, kvStr := range kvStrs {
			kvToks := strings.Split(kvStr, ": ")
			if len(kvToks) != 2 {
				continue
			}
			k, v := kvToks[0], kvToks[1]
			vInt, err := strconv.Atoi(v)
			if err != nil {
				continue
			}
			for i := 0; i < vInt; i++ {
				kvs = append(kvs, KeyValue{Key: k, Value: "1"})
			}
		}
	}
	return kvs, nil
}

// ======== task/file naming utils ========

func GetIntermediateFileName(inputFileId string, reduceIdx int) string {
	return fmt.Sprintf("%s-med-%d", inputFileId, reduceIdx)
}

func GetMapTaskId(inFileId string, inChunkId string) string {
	return fmt.Sprintf("%s-%s_map-task", inFileId, inChunkId)
}

func GetReduceTaskName(reduceIdx int) string {
	return fmt.Sprintf("%d_reduce-task", reduceIdx)
}

// used in GFS
func FileAndChunkIdToKey(fileId string, chunkId string) string {
	return fileId + "," + chunkId
}
