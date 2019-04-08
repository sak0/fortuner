FROM golang:1.11 as golang

RUN mkdir /fortuner
WORKDIR /fortuner
ENV GO111MODULE=on
ENV GOPROXY=https://goproxy.io
COPY go.mod .
COPY go.sum .
RUN go mod download
COPY . .
RUN make

FROM alpine:3.6
COPY --from=golang /fortuner/fortuner /
LABEL maintainer 61755280@qq.com
EXPOSE 17012
ENTRYPOINT ["/fortuner"]
