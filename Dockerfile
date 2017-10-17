FROM alpine
MAINTAINER seb

ENV GOPATH=/root
RUN apk update && apk add go git libc-dev

COPY kafka-consumer.go /
RUN go get github.com/bsm/sarama-cluster github.com/google/uuid
RUN go build /kafka-consumer.go
COPY start.sh /
RUN chmod 755 /start.sh

CMD /kafka-consumer.go $@
