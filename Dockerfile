# Copyright 2024 Deutsche Telekom IT GmbH
#
# SPDX-License-Identifier: Apache-2.0

FROM golang:1.22-alpine AS build
ARG GOPROXY
ARG GONOSUMDB
ENV GOPROXY=$GOPROXY
ENV GONOSUMDB=$GONOSUMDB

WORKDIR /build
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-w -s -extldflags=-static" -o scheduler .

FROM alpine:latest as certs
RUN apk --update add ca-certificates

FROM scratch
COPY --from=build /build/golaris golaris
COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
ENTRYPOINT ["./golaris", "serve"]