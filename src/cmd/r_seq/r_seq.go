package main

import (
	"flag"
	"sort"

	"cs426.yale.edu/final-proj/src/utils"
	"github.com/sirupsen/logrus"
)

/*
Run Reduce sequentially for testing.
*/

var (
	inputFiles  = flag.String("input-files", "", "Regex to match input files")
	mrPlugin    = flag.String("mr-plugin", "", "File path to plugin containing user-defined Map/Reduce function")
	outFilePath = flag.String("out", "", "File path for output")
)

func main() {
	flag.Parse()

	if len(*inputFiles) == 0 || len(*mrPlugin) == 0 || len(*outFilePath) == 0 {
		logrus.Fatal("--inputFiles, --mr-plugin, --out required")
	}

	filePaths := utils.FindAllMatchingFilePaths(*inputFiles)
	allMedKvs := make([]utils.KeyValue, 0)
	for _, outFile := range filePaths {
		kvs, err := utils.ReadFinalOutKvsFromAggUnreducedOutFile(outFile)
		if err != nil {
			return
		}
		for _, kv := range kvs {
			allMedKvs = append(allMedKvs, kv)
		}
	}

	sort.Sort(utils.SortableByKey(allMedKvs))

	start := 0
	reducedKvs := make([]utils.KeyValue, 0)
	_, reducef := utils.LoadPlugin(*mrPlugin)
	for start < len(allMedKvs) {
		end := start + 1
		for end < len(allMedKvs) && allMedKvs[start].Key == allMedKvs[end].Key {
			end++
		}
		aggValues := make([]string, 0)
		for i := start; i < end; i++ {
			aggValues = append(aggValues, allMedKvs[i].Value)
		}
		reduceOutput := reducef(allMedKvs[start].Key, aggValues)
		reducedKvs = append(reducedKvs, utils.KeyValue{
			Key:   allMedKvs[start].Key,
			Value: reduceOutput,
		})
		start = end
	}

	utils.CreateAndWriteOutKvsToFinalOutFile(*outFilePath, reducedKvs)
}
