# k8-grpc
Sample containerised GRPC Client &amp; Server

# Build the containers 

docker build -t hello_client:latest -f Dockerfile.client .
docker build -t hello_server:latest -f Dockerfile.server .

# Tag the containers

docker tag hello_client:latest  maniktaneja/hello_client:latest
docker tag hello_server:latest  maniktaneja/hello_server:latest

# Push them to the repo

docker push  maniktaneja/hello_client:latest
docker push  maniktaneja/hello_server:latest

# deploy the containers

kubectl apply -f grpc.yaml
