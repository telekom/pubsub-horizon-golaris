# Copyright 2024 Deutsche Telekom IT GmbH
#
# SPDX-License-Identifier: Apache-2.0

ARG GO_BUILD_BASE_IMAGE=golang:1.26-alpine
ARG ALPINE_BASE_IMAGE=alpine:latest

FROM ${GO_BUILD_BASE_IMAGE} AS build
ARG GOPROXY
ARG GONOSUMDB
ENV GOPROXY=$GOPROXY
ENV GONOSUMDB=$GONOSUMDB

WORKDIR /build
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-w -s -extldflags=-static" -o golaris .

FROM ${ALPINE_BASE_IMAGE} as certs
RUN apk --update add ca-certificates

FROM scratch
COPY --from=build /build/golaris golaris
COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
ENTRYPOINT ["./golaris", "serve"]
