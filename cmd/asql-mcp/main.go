// Command asql-mcp runs a Model Context Protocol server that exposes
// ASQL's unique primitives — time-travel, row history, domain
// introspection, entity versioning — to any MCP-capable agent
// (Claude Desktop, Cursor, Zed, ChatGPT Desktop, VS Code, ...).
//
// The server connects to a running asqld over pgwire and exposes a
// read-only tool surface by default.  Writes are disabled unless
// -allow-writes is passed, and even then only DML is accepted
// (no DDL).
//
// Transports:
//
//	-transport stdio       (default)  — for Claude Desktop, Claude Code,
//	                                    Cursor, Zed, etc.
//	-transport http        — streamable HTTP, for remote agents
//
// Example Claude Desktop config:
//
//	{
//	  "mcpServers": {
//	    "asql": {
//	      "command": "asql-mcp",
//	      "args": ["-pgwire", "127.0.0.1:5433"]
//	    }
//	  }
//	}
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const version = "0.1.0"

func main() {
	cfg := parseFlags()

	logger := log.New(os.Stderr, "asql-mcp: ", log.LstdFlags|log.Lmicroseconds)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	conn, err := newConn(ctx, cfg)
	if err != nil {
		logger.Fatalf("connect to pgwire: %v", err)
	}
	defer conn.Close()

	// Probe once so we fail fast with a helpful error if asqld isn't reachable.
	if err := conn.Ping(ctx); err != nil {
		logger.Fatalf("ping asqld at %s: %v", cfg.PgwireAddr, err)
	}
	logger.Printf("connected to asqld at %s (read-only=%t)", cfg.PgwireAddr, !cfg.AllowWrites)

	server := buildServer(conn, cfg)

	switch cfg.Transport {
	case "stdio":
		logger.Printf("serving MCP over stdio")
		if err := server.Run(ctx, &mcp.StdioTransport{}); err != nil && ctx.Err() == nil {
			logger.Fatalf("stdio server: %v", err)
		}
	case "http":
		handler := mcp.NewStreamableHTTPHandler(func(r *http.Request) *mcp.Server {
			return server
		}, nil)
		srv := &http.Server{Addr: cfg.HTTPAddr, Handler: handler}
		logger.Printf("serving MCP over HTTP on %s", cfg.HTTPAddr)
		go func() {
			<-ctx.Done()
			_ = srv.Shutdown(context.Background())
		}()
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatalf("http server: %v", err)
		}
	default:
		logger.Fatalf("unknown transport: %q (want stdio|http)", cfg.Transport)
	}
}

type config struct {
	PgwireAddr  string
	User        string
	Password    string
	Database    string
	Transport   string
	HTTPAddr    string
	AllowWrites bool
	MaxRows     int
}

func parseFlags() *config {
	cfg := &config{}
	flag.StringVar(&cfg.PgwireAddr, "pgwire", envOr("ASQL_PGWIRE", "127.0.0.1:5433"),
		"asqld pgwire endpoint (host:port)")
	flag.StringVar(&cfg.User, "user", envOr("ASQL_USER", "asql"),
		"pgwire user")
	flag.StringVar(&cfg.Password, "password", os.Getenv("ASQL_PASSWORD"),
		"pgwire password (empty for local asqld)")
	flag.StringVar(&cfg.Database, "database", envOr("ASQL_DATABASE", "asql"),
		"pgwire database name")
	flag.StringVar(&cfg.Transport, "transport", envOr("ASQL_MCP_TRANSPORT", "stdio"),
		"MCP transport: stdio or http")
	flag.StringVar(&cfg.HTTPAddr, "http-addr", envOr("ASQL_MCP_HTTP_ADDR", "127.0.0.1:6799"),
		"bind address for http transport")
	flag.BoolVar(&cfg.AllowWrites, "allow-writes", false,
		"allow INSERT/UPDATE/DELETE tools (default: read-only)")
	flag.IntVar(&cfg.MaxRows, "max-rows", 1000,
		"maximum rows returned per query")

	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), `asql-mcp %s — Model Context Protocol server for ASQL

Usage: asql-mcp [flags]

Flags:
`, version)
		flag.PrintDefaults()
		fmt.Fprintln(flag.CommandLine.Output(), `
Environment variables (fallback when a flag is not set):
  ASQL_PGWIRE          pgwire endpoint (default 127.0.0.1:5433)
  ASQL_USER            pgwire user (default asql)
  ASQL_PASSWORD        pgwire password
  ASQL_DATABASE        pgwire database (default asql)
  ASQL_MCP_TRANSPORT   stdio or http (default stdio)
  ASQL_MCP_HTTP_ADDR   bind for http transport (default 127.0.0.1:6799)`)
	}
	flag.Parse()

	if *showVersion {
		fmt.Println("asql-mcp", version)
		os.Exit(0)
	}
	return cfg
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
