.PHONY: all build push

all: build push

build:
	protoc batch_processor/*.proto --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative --proto_path=.
	docker build -t batch_client:latest -f Dockerfile.batch_client .
	docker build -t batch_server:latest -f Dockerfile.batch_server .

push:
	docker tag batch_client:latest maniktaneja/batch_client:latest
	docker tag batch_server:latest maniktaneja/batch_server:latest
	docker push maniktaneja/batch_client:latest
	docker push maniktaneja/batch_server:latest

