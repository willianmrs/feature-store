#!/bin/bash -xe

rm -rf ./spark3docker/target
mkdir -p ./spark3docker/target
cd ./spark3docker/target

cd -

docker build ./spark3docker/ \
    --build-arg "PARAM_UID=$(id -u)" \
    --build-arg "PARAM_GID=$(id -g)" \
    -t spark3docker
