.PHONY: proto clean test

proto:
	mkdir -p transport/proto/dkvpb
	protoc \
		--proto_path=transport/proto \
		--go_out=transport/proto/dkvpb --go_opt=paths=source_relative \
		--go-grpc_out=transport/proto/dkvpb --go-grpc_opt=paths=source_relative \
		transport/proto/dkv.proto

clean:
	rm -rf transport/proto/dkvpb/*.go

test:
	go test ./... -v -count=1