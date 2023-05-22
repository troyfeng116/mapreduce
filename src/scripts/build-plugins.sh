#!/bin/bash

cd src/plugins
for PLUGIN in ./*.go; do
    go build -race -buildmode=plugin $PLUGIN || exit 1
done

exit 0
