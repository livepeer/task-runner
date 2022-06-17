FROM livepeerci/build AS builder

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
ENV PKG_CONFIG_PATH /root/compiled/lib/pkgconfig

COPY --from=builder /root/compiled /root/compiled/
COPY --from=builder /app/build/* .

ENTRYPOINT [ "./task-runner" ]
