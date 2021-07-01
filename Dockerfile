FROM golang:1.16

WORKDIR internal/
COPY . .

RUN go env -w GOPROXY=direct
RUN go get -d -v ./...
RUN go install -v ./...

ENTRYPOINT ["go"]
CMD ["run", "internal/example/main/main.go", "-debug"]

