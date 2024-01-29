FROM golang:alpine

RUN apk update && apk add --no-cache git

WORKDIR /app

COPY . .

RUN go mod tidy

RUN GOOS=linux GOARCH=amd64 go build -o app

ENTRYPOINT ["/app/app"]