FROM golang:1.16-buster as builder

WORKDIR /app

ENV GOFLAGS "-mod=readonly"

COPY go.mod go.sum ./

RUN go mod download

ARG version
RUN echo $version

COPY . .

RUN make "version=$version"

FROM debian:buster-slim

RUN apt update \
  && apt install -y ffmpeg ca-certificates \
  && apt clean && apt autoclean
RUN ffmpeg -version
RUN update-ca-certificates

WORKDIR /app

COPY --from=builder /app/build/* .

ENTRYPOINT [ "./task-runner" ]
