# MapReduce

Yale CS426 final project

## Demo

Link to screen recording, running our distributed `MapReduce` implementation with 5 workers and 20 reduce partitions locally on 6 Project Gutenberg books, with the word count plugin [here](https://drive.google.com/file/d/1MhcwIgs8VRyiArMScyu4PK_TAsOU-3oB/view?usp=share_link).

https://drive.google.com/file/d/1MhcwIgs8VRyiArMScyu4PK_TAsOU-3oB/view?usp=share_link



## Building + running

To start GFS service:
```bash
go run src/cmd/gfs/server.go --num-workers=11 --input-files="src/files/pg-*.txt"
```

User-supplied `Map` and `Reduce` functions are loaded during run-time as Go plugins, as in MIT 6.824 Lab 1. To build a plugin:
```bash
(cd src/plugins && go build -race -buildmode=plugin wc.go) || exit 1
```

To start worker service:
```bash
go run -race src/cmd/worker/server.go --num-workers=1 --node-name=0 --mr-plugin="src/plugins/wc.so"
```

To start leader service:
```bash
go run -race src/cmd/leader/server.go --num-workers=1 --input-files="src/files/pg-*.txt"
```

To run distributed `MapReduce` all at once:
```bash
# src/scripts/mr.sh [num_nodes] [input_files] [num_reduce] [plugin] [timeout_seconds] [out_file_id]
./src/scripts/mr.sh 5 "src/files/pg-*.txt" 9 "src/plugins/wc.so" 22 "wc-out"
```

To run `MapReduce` tests (based on abstracted functionality):
```bash
./src/scripts/test-mr.sh [optional_num-nodes] [optional_num-reduce]
```



## TODOs
- gRPC stream protobuf for mock GFS?
- don't use regex to match input files
