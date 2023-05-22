#!/bin/bash

# usage: mr.sh [num_nodes] [input_files] [num_reduce] [plugin] [timeout_seconds] [out_file_id]
# ex. mr.sh 11 "src/files/pg-*.txt" 9 "src/plugins/wc.so" 20
# writes aggregated output to `out/mr-out_[out_file_id].out``

if [ -z "$6" ]; then
    echo "usage: mr.sh [num_nodes] [input_files] [num_reduce] [plugin] [timeout_seconds] [out_file_id]"
    exit 1
fi

NUM_NODES=$1
INPUT_FILES=$2
NUM_REDUCE=$3
PLUGIN=$4
TIMEOUT_SECONDS=$5
OUT_FILE_ID=$6

[ ! -d "out" ] && mkdir out;
make || exit 1;

# build plugins
./src/scripts/build-plugins.sh || exit 1;

# ======== distributed MapReduce ========

PIDS=()

# taken from https://unix.stackexchange.com/questions/124127/kill-all-descendant-processes
list_descendants () {
    local children=$(ps -o pid= --ppid "$1")

    for pid in $children; do
        list_descendants "$pid"
    done

    echo "$children"
}

# usage: clear_pids $PIDS
clear_pids() {
    # kill services
    LOCAL_PIDS=("$@")
    echo "${LOCAL_PIDS[@]}"
    for pid in "${LOCAL_PIDS[@]}"; do
        pkill -P $pid
        # echo "killing $pid and descendants"
        # if [ ! -z $pid ]; then
        #     kill $(list_descendants $pid)
        # fi
    done
    # pkill -P $$
}

# start GFS
go run -race src/cmd/gfs/server.go --num-workers=$NUM_NODES --input-files=$INPUT_FILES &
PIDS+=($!)

# start workers
for (( i=0; i<$1; i++ )); do
    go run -race src/cmd/worker/server.go --num-workers=$NUM_NODES --node-name=$i --num-reduce=$NUM_REDUCE --mr-plugin=$PLUGIN &
    PIDS+=($!)
done

# start leader
go run -race src/cmd/leader/server.go --num-workers=$NUM_NODES --input-files=$INPUT_FILES --num-reduce=$NUM_REDUCE --out-file=$OUT_FILE_ID &
PIDS+=($!)

# wait for MapReduce to complete
sleep $TIMEOUT_SECONDS;

clear_pids "${PIDS[@]}";

# aggregate all MapReduce output files
UNREDUCED_AGG_OUT=out/mr-out_unreduced_$OUT_FILE_ID.out
sort out/mr-out_$OUT_FILE_ID*.out > $UNREDUCED_AGG_OUT
AGG_OUT=out/mr-out_$OUT_FILE_ID.out
go run -race src/cmd/r_seq/r_seq.go --input-files=$UNREDUCED_AGG_OUT --mr-plugin=$PLUGIN --out=$AGG_OUT
exit 0
