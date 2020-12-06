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

all: build copy-artefacts docker-build
