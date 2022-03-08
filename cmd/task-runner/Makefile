version ?= $(shell git describe --tag --dirty)

ldflags := -X 'main.Version=$(version)'
builddir := ../../build

.PHONY: all build run

all: build

build:
	go build -o $(builddir)/task-runner -ldflags="$(ldflags)" main/main.go

run:
	go run -ldflags="$(ldflags)" main/main.go $(args)
