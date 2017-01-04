FROM golang:1.7

RUN mkdir -p /go/src/app \
	&& cd /go/src/app \
	&& go get github.com/gocql/gocql

RUN mkdir -p /go/src/app \
	&& cd /go/src/app \
	&& go get github.com/Shopify/sarama

COPY . /go/src/app

RUN cd /go/src/app \
	&& go build .

RUN mv /go/src/app/app /usr/local/bin/

CMD ["app"]
