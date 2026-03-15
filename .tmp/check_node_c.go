package main

import (
  "fmt"
  "log/slog"
  "os"
  pgwireserver "asql/internal/server/pgwire"
)

func main() {
  logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
  _, err := pgwireserver.New(pgwireserver.Config{
    Address: ":5435",
    AdminHTTPAddr: ":9093",
    DataDirPath: ".asql-node-c",
    Logger: logger,
    NodeID: "node-c",
    ClusterGRPCAddr: ":6435",
    Peers: []string{"node-a@127.0.0.1:6433", "node-b@127.0.0.1:6434"},
    Groups: []string{"default"},
  })
  if err != nil {
    fmt.Printf("ERR: %v\n", err)
    os.Exit(1)
  }
  fmt.Println("OK")
}
