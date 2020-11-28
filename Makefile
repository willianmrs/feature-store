.PHONY: test build compile

SBT_CLIENT := $(shell which sbt)

test:
	@$(SBT_CLIENT) clean test

build:
	@$(SBT_CLIENT) assembly

compile:
	@$(SBT_CLIENT) clean compile

