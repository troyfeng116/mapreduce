.PHONY: compile

compile:
	protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative src/gfs/proto/gfs.proto src/mr/worker/proto/worker.proto src/mr/leader/proto/leader.proto
