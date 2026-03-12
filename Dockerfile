FROM golang:1.24 AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/asqld ./cmd/asqld
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/asqlctl ./cmd/asqlctl

FROM gcr.io/distroless/static-debian12

WORKDIR /app

COPY --from=build /out/asqld /usr/local/bin/asqld
COPY --from=build /out/asqlctl /usr/local/bin/asqlctl

EXPOSE 9042

ENTRYPOINT ["/usr/local/bin/asqld"]
CMD ["-addr", ":9042", "-data-dir", "/data/.asql"]