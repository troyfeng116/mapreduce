#!/bin/bash

# usage: test-mr.sh [num_nodes] [num_reduce]

NUM_NODES=$1
if [ -z $NUM_NODES ]; then
    NUM_NODES=11
fi
# TODO: add more plugins (final reduce is annoying)
PLUGINS=("wc")
INPUT_FILES="src/files/pg-*.txt"
NUM_REDUCE=$2
if [ -z $NUM_REDUCE ]; then
    NUM_REDUCE=15
fi
TIMEOUT=30

for PLUGIN in "${PLUGINS[@]}"; do
    [ -d "out" ] && rm -rf out;
    [ ! -d "out" ] && mkdir out;

    # build plugins
    ./src/scripts/build-plugins.sh || exit 1;

    PLUGIN_FILE="src/plugins/$PLUGIN.so"

    # run sequentially first
    CORRECT_OUT="out/mr-seq-out_correct_$PLUGIN.out"
    # run sequentially first
    go run -race src/cmd/seq/mr_seq.go --input-files=$INPUT_FILES --mr-plugin=$PLUGIN_FILE --out=$CORRECT_OUT || exit 1;

    # see mr.sh for output naming conventions
    MR_OUT="out/mr-out_$PLUGIN.out"
    ./src/scripts/mr.sh $NUM_NODES "$INPUT_FILES" $NUM_REDUCE "$PLUGIN_FILE" $TIMEOUT $PLUGIN || exit 1;

    if cmp $MR_OUT $CORRECT_OUT
    then
        echo '========' $PLUGIN test: PASS
    else
        echo '========' $PLUGIN output in $MR_OUT is not the same as $CORRECT_OUT
        echo '========' $PLUGIN test: FAIL
        exit 1
    fi
done

exit 0
