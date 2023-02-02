FROM	golang:1.20-buster	as	builder

WORKDIR	/app

ENV	GOFLAGS	"-mod=readonly"

COPY	go.mod	go.sum	./

RUN	go mod download

ARG	version
RUN	echo $version

COPY	.	.

RUN	make "version=$version"

FROM	debian:buster-slim

RUN	apt update \
	&& apt install -yqq ffmpeg ca-certificates \
	&& apt clean \
	&& apt autoclean

RUN	ffmpeg -version \
	&& update-ca-certificates

WORKDIR	/app

COPY --from=builder	/app/build/*	/usr/local/bin/

ENTRYPOINT	["/usr/local/bin/task-runner"]
