#!/bin/bash
set -ef

IMAGE_TAG=${1:-latest}

export BUILD_CONTAINER_NAME=kafka-grpc-proxy-build:${IMAGE_TAG}
export EXTRACT_CONTAINER=kafka-grpc-proxy-extract-${IMAGE_TAG}
export TAG_NAME=kafka-grpc-proxy:${IMAGE_TAG}


rm -rf ./extract
mkdir -p ./extract/bin
mkdir -p ./extract/lib
mkdir -p ./extract/lib64
echo "removing old extract container"
docker rm -f $EXTRACT_CONTAINER || true

pushd ..
  docker build --build-arg IMAGE_TAG=${IMAGE_TAG} --file docker/Dockerfile.build --tag $BUILD_CONTAINER_NAME .
popd

docker create --name $EXTRACT_CONTAINER $BUILD_CONTAINER_NAME

docker cp $EXTRACT_CONTAINER:/usr/local/lib                                 ./extract
echo $PWD
find ./extract -name "*.a" -exec rm -rf {} \;

docker cp $EXTRACT_CONTAINER:/usr/local/bin/kafka-grpc-proxy           ./extract/bin
docker cp $EXTRACT_CONTAINER:/usr/local/bin/postgres2kafka             ./extract/bin
docker cp $EXTRACT_CONTAINER:/src/runDeps                              ./extract/runDeps

docker rm -f $EXTRACT_CONTAINER

docker build -f Dockerfile --no-cache -t$TAG_NAME .

rm -rf ./extract


