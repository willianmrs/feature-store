.PHONY: test build compile

SBT_CLIENT := $(shell which sbt)

test:
	@$(SBT_CLIENT) clean test

build:
	@$(SBT_CLIENT) clean assembly

compile:
	@$(SBT_CLIENT) clean compile

docker-build:
	@bash ./spark3docker/build.sh

copy-artefacts:
	cp target/scala-2.12/ifood-data-ingestion.jar ./spark3docker/artefact

build-all: build copy-artefacts docker-build

docker-run:
	docker run -it --net host --rm=true -e DISPLAY=$DISPLAY -v /tmp/.X11-unix:/tmp/.X11-unix -v $(pwd):/home/ifood/project -p 8888:8888 spark3docker

run-mysql:
	docker run --rm --name ifood-ml-engineer -p 3306:3306 -e MYSQL_ROOT_HOST=% -e MYSQL_ROOT_PASSWORD=root -d mysql:latest
stop-mysql:
	docker stop ifood-ml-engineer || true && docker rm ifood-ml-engineer || true
