FROM golang:1.16-stretch as builder

WORKDIR /app
ENV PKG_CONFIG_PATH /root/compiled/lib/pkgconfig

RUN apt update \
  && apt install -y build-essential pkg-config autoconf gnutls-dev git curl

RUN curl -s https://raw.githubusercontent.com/livepeer/go-livepeer/5da8ff8e521c15e5828fb9dfa619daf91a091fd0/install_ffmpeg.sh \
  | bash -

ENV GOFLAGS "-mod=readonly"

COPY go.mod go.sum ./

RUN go mod download

ARG version
RUN echo $version

COPY . .

RUN make "version=$version"

FROM debian:stretch-slim

RUN apt update \
  && apt install -y ffmpeg ca-certificates \
  && apt clean && apt autoclean
RUN ffmpeg -version
RUN update-ca-certificates

WORKDIR /app
ENV PKG_CONFIG_PATH /root/compiled/lib/pkgconfig

COPY --from=builder /root/compiled /root/compiled/
COPY --from=builder /app/build/* .

ENTRYPOINT [ "./task-runner" ]
