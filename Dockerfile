FROM	golang:1.22-bookworm	as	builder

WORKDIR	/app

ENV	GOFLAGS	"-mod=readonly"

COPY	go.mod	go.sum	./

RUN	go mod download

ARG	version
RUN	echo $version

COPY	.	.

RUN	make "version=$version"

FROM	debian:bookworm-slim

RUN	apt update \
	&& apt install -yqq ca-certificates \
	&& apt clean \
	&& apt autoclean

RUN	update-ca-certificates

WORKDIR	/app

COPY --from=builder	/app/build/*	/usr/local/bin/

ENTRYPOINT	["/usr/local/bin/task-runner"]
