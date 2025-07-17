# Copyright 2024 Deutsche Telekom IT GmbH
#
# SPDX-License-Identifier: Apache-2.0

FROM golang:1.23-alpine AS build
ARG GOPROXY
ARG GONOSUMDB
ENV GOPROXY=$GOPROXY
ENV GONOSUMDB=$GONOSUMDB

WORKDIR /build
COPY . .
RUN apk add --no-cache gcc libc-dev librdkafka-dev
RUN CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -tags musl -ldflags="-w -s -extldflags=-static" -o golaris .

FROM alpine:latest as certs
RUN apk --update add ca-certificates

FROM scratch
COPY --from=build /build/golaris golaris
COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
ENTRYPOINT ["./golaris", "serve"]