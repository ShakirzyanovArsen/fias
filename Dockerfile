FROM golang:1.11-alpine AS build
RUN mkdir /go/src/fias
RUN apk add --no-cache git
RUN go get -u github.com/golang/dep/cmd/dep
COPY ./ /go/src/fias
WORKDIR /go/src/fias
RUN dep ensure
ENV CGO_ENABLED=0
RUN go test -v && go build -o app
RUN apk add -U --no-cache ca-certificates

FROM alpine:3.8
WORKDIR /
COPY --from=build /go/src/fias/app /
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
RUN mkdir /data
CMD []