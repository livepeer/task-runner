cmd ?= task-runner
version ?= $(shell git describe --tag --dirty)
tags ?= latest $(version)

allCmds := $(shell ls ./cmd/)
dockerimg := livepeer/task-runner

.PHONY: all $(allCmds) docker docker_run docker_push

all: $(allCmds)

$(allCmds):
	$(MAKE) -C ./cmd/$@

run:
	$(MAKE) -C ./cmd/$(cmd) run

docker:
	docker build $(foreach tag,$(tags),-t $(dockerimg):$(tag)) --build-arg version=$(version) .

docker_run: docker
	docker run -it --rm --name=$(cmd) --network=host $(dockerimg) $(args)

docker_push:
	for TAG in $(tags) ; \
	do \
		docker push $(dockerimg):$$TAG ; \
	done;

docker_ci:
	docker buildx build --push --platform linux/amd64,linux/arm64 $(foreach tag,$(tags),-t $(dockerimg):$(tag)) --build-arg version=$(version) .
