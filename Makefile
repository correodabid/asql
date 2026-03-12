SHELL := /bin/sh

.PHONY: tidy fmt vet test test-race bench bench-core bench-write bench-wal bench-pgwire \
	bench-env-single bench-env-cluster bench-run-single bench-run-cluster \
	bench-seed-single bench-seed-cluster bench-matrix-single bench-matrix-cluster \
	bench-append-growth bench-append-growth-single bench-append-growth-cluster bench-append-growth-cluster-guardrail \
	bench-seed-degradation bench-seed-degradation-single bench-seed-degradation-cluster \
	run \
        studio-web-install studio-web-build run-studio \
        dev dev-cluster kill-dev restart-db \
        kill-node-a kill-node-b kill-node-c kill-node-d \
        start-node-a start-node-b start-node-c start-node-d \
        restart-node-a restart-node-b restart-node-c restart-node-d \
        restart-logic restart-full \
        seed-domains ci clean

BENCH_TX ?= 1200
BENCH_WORKERS ?= 4
BENCH_STEPS ?= 5
BENCH_MATERIALS ?= 3
BENCH_INDEXES ?= true
BENCH_SCENARIO ?= default
BENCH_DEG_CMD ?= go run ./scripts/seed_domains
BENCH_DEG_ROUNDS ?= 8
BENCH_DEG_WARMUP ?= 1
BENCH_DEG_FAIL_RATIO ?= 2.00
BENCH_APPEND_ADDR ?= 127.0.0.1:5433
BENCH_APPEND_ROUNDS ?= 8
BENCH_APPEND_WARMUP ?= 1
BENCH_APPEND_ROWS ?= 20000
BENCH_APPEND_BATCH ?= 250
BENCH_APPEND_PAYLOAD ?= 96
BENCH_APPEND_SECONDARY_INDEX ?= true
BENCH_APPEND_FAIL_RATIO ?= 1.50
DEV_ADMIN_ADDR_A ?= :9091
DEV_ADMIN_ADDR_B ?= :9092
DEV_ADMIN_ADDR_C ?= :9093
DEV_ADMIN_ADDR_D ?= :9094

# ── Go ─────────────────────────────────────────────────────────────────────

tidy:
	go mod tidy

fmt:
	gofmt -w $$(find . -type f -name '*.go')

vet:
	go vet ./...

test:
	go test ./...

test-race:
	go test -race ./...

bench:
	go test -run '^$$' -bench BenchmarkEngine -benchmem ./internal/engine/executor
	go test -run '^$$' -bench BenchmarkFileLogStore -benchmem ./internal/storage/wal

bench-core: bench-write bench-wal

bench-write:
	go test -run '^$$' -bench 'BenchmarkEngineWrite|BenchmarkEngineReadAsOfLSN|BenchmarkEngineReplayToLSN' -benchmem ./internal/engine/executor

bench-wal:
	go test -run '^$$' -bench BenchmarkFileLogStore -benchmem ./internal/storage/wal

bench-pgwire:
	go test -run '^$$' -bench BenchmarkPGWire -benchmem ./internal/server/pgwire

bench-env-single: kill-dev restart-db
	@sleep 0.5
	@rm -f /tmp/asql-bench-single.log
	@go build -o /tmp/asqld ./cmd/asqld
	@/tmp/asqld -addr :5433 -data-dir .asql > /tmp/asql-bench-single.log 2>&1 & \
		PID=$$!; \
		OK=0; \
		for i in $$(seq 1 60); do \
			if lsof -ti :5433 >/dev/null 2>&1; then OK=1; break; fi; \
			if ! kill -0 $$PID 2>/dev/null; then echo "bench single node crashed:"; cat /tmp/asql-bench-single.log; exit 1; fi; \
			sleep 1; \
		done; \
		if [ $$OK -eq 0 ]; then echo "bench single node timeout"; cat /tmp/asql-bench-single.log; exit 1; fi
	@echo "bench single-node env ready | logs: /tmp/asql-bench-single.log"

bench-env-cluster: kill-dev restart-db
	@sleep 0.5
	@rm -f /tmp/asql-bench-node-a.log /tmp/asql-bench-node-b.log /tmp/asql-bench-node-c.log
	@go build -o /tmp/asqld ./cmd/asqld
	@/tmp/asqld -addr :5433 -data-dir .asql-node-a \
		-node-id node-a -grpc-addr :6433 \
		-peers "node-b@127.0.0.1:6434,node-c@127.0.0.1:6435" \
		-groups default > /tmp/asql-bench-node-a.log 2>&1 & \
		PID_A=$$!; \
		OK=0; \
		for i in $$(seq 1 60); do \
			if lsof -ti :5433 >/dev/null 2>&1; then OK=1; break; fi; \
			if ! kill -0 $$PID_A 2>/dev/null; then echo "bench node-a crashed:"; cat /tmp/asql-bench-node-a.log; exit 1; fi; \
			sleep 1; \
		done; \
		if [ $$OK -eq 0 ]; then echo "bench node-a timeout"; cat /tmp/asql-bench-node-a.log; exit 1; fi
	@/tmp/asqld -addr :5434 -data-dir .asql-node-b \
		-node-id node-b -grpc-addr :6434 \
		-peers "node-a@127.0.0.1:6433,node-c@127.0.0.1:6435" \
		-groups default > /tmp/asql-bench-node-b.log 2>&1 & \
		PID_B=$$!; \
		OK=0; \
		for i in $$(seq 1 15); do \
			if lsof -ti :5434 >/dev/null 2>&1; then OK=1; break; fi; \
			if ! kill -0 $$PID_B 2>/dev/null; then echo "bench node-b crashed:"; cat /tmp/asql-bench-node-b.log; exit 1; fi; \
			sleep 1; \
		done; \
		if [ $$OK -eq 0 ]; then echo "bench node-b timeout"; cat /tmp/asql-bench-node-b.log; exit 1; fi
	@/tmp/asqld -addr :5435 -data-dir .asql-node-c \
		-node-id node-c -grpc-addr :6435 \
		-peers "node-a@127.0.0.1:6433,node-b@127.0.0.1:6434" \
		-groups default > /tmp/asql-bench-node-c.log 2>&1 & \
		PID_C=$$!; \
		OK=0; \
		for i in $$(seq 1 15); do \
			if lsof -ti :5435 >/dev/null 2>&1; then OK=1; break; fi; \
			if ! kill -0 $$PID_C 2>/dev/null; then echo "bench node-c crashed:"; cat /tmp/asql-bench-node-c.log; exit 1; fi; \
			sleep 1; \
		done; \
		if [ $$OK -eq 0 ]; then echo "bench node-c timeout"; cat /tmp/asql-bench-node-c.log; exit 1; fi
	@echo "bench cluster env ready | logs: /tmp/asql-bench-node-a.log /tmp/asql-bench-node-b.log /tmp/asql-bench-node-c.log"

# ── Engine ──────────────────────────────────────────────────────────────────

run:
	go run ./cmd/asqld -addr :5433 -data-dir .asql

# ── Studio (desktop) ─────────────────────────────────────────────────────────

studio-web-install:
	cd ./cmd/asqlstudio/webapp && npm install

studio-web-build:
	cd ./cmd/asqlstudio/webapp && npm run build

run-studio:
	@echo "asqld must already be running (make run)"
	cd ./cmd/asqlstudio && ASQL_PGWIRE_ENDPOINT=127.0.0.1:5433 wails dev

# run-studio-cluster: Studio pre-seeded with all three dev-cluster node endpoints.
# Survives any single node failure (including the initial leader) without relying
# on autodiscovery completing before the failure window.
run-studio-cluster:
	@echo "dev-cluster must already be running (make dev-cluster)"
	cd ./cmd/asqlstudio && \
		ASQL_PGWIRE_ENDPOINT=127.0.0.1:5433 \
		ASQL_FOLLOWER_ENDPOINT=127.0.0.1:5434 \
		ASQL_PEER_ENDPOINTS=127.0.0.1:5433,127.0.0.1:5434,127.0.0.1:5435 \
		ASQL_ADMIN_ENDPOINTS=127.0.0.1$$(printf '%s' $(DEV_ADMIN_ADDR_A)),127.0.0.1$$(printf '%s' $(DEV_ADMIN_ADDR_B)),127.0.0.1$$(printf '%s' $(DEV_ADMIN_ADDR_C)) \
		ASQL_GROUPS=default \
		wails dev

# ── Dev environments ─────────────────────────────────────────────────────────

dev:
	@lsof -ti TCP:5433 -sTCP:LISTEN | xargs kill -9 2>/dev/null || true
	@sleep 0.5
	@rm -f /tmp/asql-dev-node.log
	@go build -o /tmp/asqld ./cmd/asqld
	@/tmp/asqld -addr :5433 -data-dir .asql -pprof-addr :6060 > /tmp/asql-dev-node.log 2>&1 & \
		PID=$$!; \
		OK=0; \
		for i in $$(seq 1 60); do \
			if lsof -ti :5433 >/dev/null 2>&1; then OK=1; break; fi; \
			if ! kill -0 $$PID 2>/dev/null; then echo "asqld crashed:"; cat /tmp/asql-dev-node.log; exit 1; fi; \
			sleep 1; \
		done; \
		if [ $$OK -eq 0 ]; then echo "asqld timeout"; cat /tmp/asql-dev-node.log; exit 1; fi
	cd ./cmd/asqlstudio && ASQL_PGWIRE_ENDPOINT=127.0.0.1:5433 wails dev

dev-cluster: kill-dev
	@sleep 0.5
	@rm -f /tmp/asql-dev-node-a.log /tmp/asql-dev-node-b.log /tmp/asql-dev-node-c.log
	@rm -rf .asql-node-a .asql-node-b .asql-node-c
	@go build -o /tmp/asqld ./cmd/asqld
	@/tmp/asqld -addr :5433 -data-dir .asql-node-a \
		-admin-addr $(DEV_ADMIN_ADDR_A) \
		-node-id node-a -grpc-addr :6433 \
		-peers "node-b@127.0.0.1:6434,node-c@127.0.0.1:6435" \
		-groups default > /tmp/asql-dev-node-a.log 2>&1 & \
		PID_A=$$!; \
		OK=0; \
		for i in $$(seq 1 60); do \
			if lsof -ti :5433 >/dev/null 2>&1; then OK=1; break; fi; \
			if ! kill -0 $$PID_A 2>/dev/null; then echo "node-a crashed:"; cat /tmp/asql-dev-node-a.log; exit 1; fi; \
			sleep 1; \
		done; \
		if [ $$OK -eq 0 ]; then echo "node-a timeout"; cat /tmp/asql-dev-node-a.log; exit 1; fi
	@/tmp/asqld -addr :5434 -data-dir .asql-node-b \
		-admin-addr $(DEV_ADMIN_ADDR_B) \
		-node-id node-b -grpc-addr :6434 \
		-peers "node-a@127.0.0.1:6433,node-c@127.0.0.1:6435" \
		-groups default > /tmp/asql-dev-node-b.log 2>&1 & \
		PID_B=$$!; \
		OK=0; \
		for i in $$(seq 1 15); do \
			if lsof -ti :5434 >/dev/null 2>&1; then OK=1; break; fi; \
			if ! kill -0 $$PID_B 2>/dev/null; then echo "node-b crashed:"; cat /tmp/asql-dev-node-b.log; exit 1; fi; \
			sleep 1; \
		done; \
		if [ $$OK -eq 0 ]; then echo "node-b timeout"; cat /tmp/asql-dev-node-b.log; exit 1; fi
	@/tmp/asqld -addr :5435 -data-dir .asql-node-c \
		-admin-addr $(DEV_ADMIN_ADDR_C) \
		-node-id node-c -grpc-addr :6435 \
		-peers "node-a@127.0.0.1:6433,node-b@127.0.0.1:6434" \
		-groups default > /tmp/asql-dev-node-c.log 2>&1 & \
		PID_C=$$!; \
		OK=0; \
		for i in $$(seq 1 15); do \
			if lsof -ti :5435 >/dev/null 2>&1; then OK=1; break; fi; \
			if ! kill -0 $$PID_C 2>/dev/null; then echo "node-c crashed:"; cat /tmp/asql-dev-node-c.log; exit 1; fi; \
			sleep 1; \
		done; \
		if [ $$OK -eq 0 ]; then echo "node-c timeout"; cat /tmp/asql-dev-node-c.log; exit 1; fi
	@echo "metrics: node-a=http://127.0.0.1$$(printf '%s' $(DEV_ADMIN_ADDR_A)) node-b=http://127.0.0.1$$(printf '%s' $(DEV_ADMIN_ADDR_B)) node-c=http://127.0.0.1$$(printf '%s' $(DEV_ADMIN_ADDR_C))"
	cd ./cmd/asqlstudio && \
		ASQL_PGWIRE_ENDPOINT=127.0.0.1:5433 \
		ASQL_PEER_ENDPOINTS=127.0.0.1:5433,127.0.0.1:5434,127.0.0.1:5435 \
		ASQL_ADMIN_ENDPOINTS=127.0.0.1$$(printf '%s' $(DEV_ADMIN_ADDR_A)),127.0.0.1$$(printf '%s' $(DEV_ADMIN_ADDR_B)),127.0.0.1$$(printf '%s' $(DEV_ADMIN_ADDR_C)) \
		ASQL_GROUPS=default \
		wails dev

# fresh-cluster: alias for dev-cluster (always starts with clean data dirs).
fresh-cluster: dev-cluster

kill-dev:
	@lsof -ti TCP:5433 -sTCP:LISTEN | xargs kill 2>/dev/null || true
	@lsof -ti TCP:5434 -sTCP:LISTEN | xargs kill 2>/dev/null || true
	@lsof -ti TCP:5435 -sTCP:LISTEN | xargs kill 2>/dev/null || true
	@lsof -ti TCP:5436 -sTCP:LISTEN | xargs kill 2>/dev/null || true
	@lsof -ti TCP:6433 -sTCP:LISTEN | xargs kill 2>/dev/null || true
	@lsof -ti TCP:6434 -sTCP:LISTEN | xargs kill 2>/dev/null || true
	@lsof -ti TCP:6435 -sTCP:LISTEN | xargs kill 2>/dev/null || true
	@lsof -ti TCP:6436 -sTCP:LISTEN | xargs kill 2>/dev/null || true
	@lsof -ti TCP:9091 -sTCP:LISTEN | xargs kill 2>/dev/null || true
	@lsof -ti TCP:9092 -sTCP:LISTEN | xargs kill 2>/dev/null || true
	@lsof -ti TCP:9093 -sTCP:LISTEN | xargs kill 2>/dev/null || true
	@lsof -ti TCP:9094 -sTCP:LISTEN | xargs kill 2>/dev/null || true
	@lsof -ti TCP:5173 -sTCP:LISTEN | xargs kill 2>/dev/null || true
	@lsof -ti TCP:6060 -sTCP:LISTEN | xargs kill 2>/dev/null || true
	@pkill -f 'wails dev' 2>/dev/null || true

# dev-join: hot-join a 4th node into a running dev-cluster.
# Usage: first run `make dev-cluster`, then in another shell: `make dev-join`
dev-join:
	@go build -o /tmp/asqld ./cmd/asqld
	@/tmp/asqld -addr :5436 -data-dir .asql-node-d \
		-admin-addr $(DEV_ADMIN_ADDR_D) \
		-node-id node-d -grpc-addr :6436 \
		-join 127.0.0.1:6433 \
		-groups default > /tmp/asql-dev-node-d.log 2>&1 &
	@echo "node-d starting on pgwire :5436, gRPC :6436, metrics $(DEV_ADMIN_ADDR_D) (joining via node-a at :6433)"
	@echo "Logs: tail -f /tmp/asql-dev-node-d.log"

# ── Per-node kill / start / restart ──────────────────────────────────────────
# Useful for simulating node failure and recovery without tearing down the
# whole cluster.  start-node-X re-uses the existing data dir so it rejoins
# as a follower and replays WAL from the leader.

kill-node-a:
	@lsof -ti TCP:5433 -sTCP:LISTEN | xargs kill 2>/dev/null || true
	@lsof -ti TCP:6433 -sTCP:LISTEN | xargs kill 2>/dev/null || true
	@echo "node-a killed (pgwire :5433 / gRPC :6433)"

kill-node-b:
	@lsof -ti TCP:5434 -sTCP:LISTEN | xargs kill 2>/dev/null || true
	@lsof -ti TCP:6434 -sTCP:LISTEN | xargs kill 2>/dev/null || true
	@echo "node-b killed (pgwire :5434 / gRPC :6434)"

kill-node-c:
	@lsof -ti TCP:5435 -sTCP:LISTEN | xargs kill 2>/dev/null || true
	@lsof -ti TCP:6435 -sTCP:LISTEN | xargs kill 2>/dev/null || true
	@echo "node-c killed (pgwire :5435 / gRPC :6435)"

kill-node-d:
	@lsof -ti TCP:5436 -sTCP:LISTEN | xargs kill 2>/dev/null || true
	@lsof -ti TCP:6436 -sTCP:LISTEN | xargs kill 2>/dev/null || true
	@echo "node-d killed (pgwire :5436 / gRPC :6436)"

start-node-a:
	@/tmp/asqld -addr :5433 -data-dir .asql-node-a \
		-admin-addr $(DEV_ADMIN_ADDR_A) \
		-node-id node-a -grpc-addr :6433 \
		-peers "node-b@127.0.0.1:6434,node-c@127.0.0.1:6435" \
		-groups default >> /tmp/asql-dev-node-a.log 2>&1 &
	@echo "node-a started (metrics http://127.0.0.1$$(printf '%s' $(DEV_ADMIN_ADDR_A))) | logs: tail -f /tmp/asql-dev-node-a.log"

start-node-b:
	@/tmp/asqld -addr :5434 -data-dir .asql-node-b \
		-admin-addr $(DEV_ADMIN_ADDR_B) \
		-node-id node-b -grpc-addr :6434 \
		-peers "node-a@127.0.0.1:6433,node-c@127.0.0.1:6435" \
		-groups default >> /tmp/asql-dev-node-b.log 2>&1 &
	@echo "node-b started (metrics http://127.0.0.1$$(printf '%s' $(DEV_ADMIN_ADDR_B))) | logs: tail -f /tmp/asql-dev-node-b.log"

start-node-c:
	@/tmp/asqld -addr :5435 -data-dir .asql-node-c \
		-admin-addr $(DEV_ADMIN_ADDR_C) \
		-node-id node-c -grpc-addr :6435 \
		-peers "node-a@127.0.0.1:6433,node-b@127.0.0.1:6434" \
		-groups default >> /tmp/asql-dev-node-c.log 2>&1 &
	@echo "node-c started (metrics http://127.0.0.1$$(printf '%s' $(DEV_ADMIN_ADDR_C))) | logs: tail -f /tmp/asql-dev-node-c.log"

start-node-d:
	@/tmp/asqld -addr :5436 -data-dir .asql-node-d \
		-admin-addr $(DEV_ADMIN_ADDR_D) \
		-node-id node-d -grpc-addr :6436 \
		-join 127.0.0.1:6433 \
		-groups default >> /tmp/asql-dev-node-d.log 2>&1 &
	@echo "node-d started (metrics http://127.0.0.1$$(printf '%s' $(DEV_ADMIN_ADDR_D))) | logs: tail -f /tmp/asql-dev-node-d.log"

restart-node-a: kill-node-a
	@sleep 0.5
	$(MAKE) start-node-a

restart-node-b: kill-node-b
	@sleep 0.5
	$(MAKE) start-node-b

restart-node-c: kill-node-c
	@sleep 0.5
	$(MAKE) start-node-c

restart-node-d: kill-node-d
	@sleep 0.5
	$(MAKE) start-node-d

restart-db:
	@rm -rf .asql .asql-node-a .asql-node-b .asql-node-c .asql-node-d

restart-logic:
	$(MAKE) kill-dev
	@sleep 1
	$(MAKE) dev

restart-full:
	$(MAKE) kill-dev
	@sleep 1
	$(MAKE) restart-db
	@sleep 1
	$(MAKE) dev



# ── Seed ─────────────────────────────────────────────────────────────────────

seed-domains:
	go run ./scripts/seed_domains

seed-domains-x10:
	go run ./scripts/seed_domains -workers 8 -scale 10

bench-run-single:
	@SUFFIX=$$(date +%s); \
	DOMAIN_NAME=bench_write_single_$(BENCH_SCENARIO)_$$SUFFIX; \
	echo "single-node benchmark scenario: $(BENCH_SCENARIO) | domain: $$DOMAIN_NAME"; \
	go run ./scripts/bench_write_workload \
		-pgwire-addr 127.0.0.1:5433 \
		-cluster-addrs 127.0.0.1:5433 \
		-scenario $(BENCH_SCENARIO) \
		-domain $$DOMAIN_NAME \
		-workers $(BENCH_WORKERS) \
		-transactions $(BENCH_TX) \
		-steps-per-batch $(BENCH_STEPS) \
		-materials-per-batch $(BENCH_MATERIALS) \
		-with-indexes=$(BENCH_INDEXES)

bench-run-cluster:
	@SUFFIX=$$(date +%s); \
	DOMAIN_NAME=bench_write_cluster_$(BENCH_SCENARIO)_$$SUFFIX; \
	echo "cluster benchmark scenario: $(BENCH_SCENARIO) | domain: $$DOMAIN_NAME"; \
	go run ./scripts/bench_write_workload \
		-pgwire-addr 127.0.0.1:5433 \
		-cluster-addrs 127.0.0.1:5433,127.0.0.1:5434,127.0.0.1:5435 \
		-scenario $(BENCH_SCENARIO) \
		-domain $$DOMAIN_NAME \
		-workers $(BENCH_WORKERS) \
		-transactions $(BENCH_TX) \
		-steps-per-batch $(BENCH_STEPS) \
		-materials-per-batch $(BENCH_MATERIALS) \
		-with-indexes=$(BENCH_INDEXES)

bench-seed-single: bench-env-single bench-run-single

bench-seed-cluster: bench-env-cluster bench-run-cluster

bench-seed-degradation:
	go run ./scripts/bench_seed_degradation \
		-cmd "$(BENCH_DEG_CMD)" \
		-rounds $(BENCH_DEG_ROUNDS) \
		-warmup $(BENCH_DEG_WARMUP) \
		-fail-ratio $(BENCH_DEG_FAIL_RATIO)

bench-seed-degradation-single: bench-env-single
	$(MAKE) bench-seed-degradation \
		BENCH_DEG_CMD="go run ./scripts/seed_domains -pgwire-addr 127.0.0.1:5433 -cluster-addrs 127.0.0.1:5433"

bench-seed-degradation-cluster: bench-env-cluster
	$(MAKE) bench-seed-degradation \
		BENCH_DEG_CMD="go run ./scripts/seed_domains -pgwire-addr 127.0.0.1:5433 -cluster-addrs 127.0.0.1:5433,127.0.0.1:5434,127.0.0.1:5435"

bench-append-growth:
	go run ./scripts/bench_append_growth \
		-pgwire-addr $(BENCH_APPEND_ADDR) \
		-cluster-addrs "$(BENCH_APPEND_CLUSTER_ADDRS)" \
		-rounds $(BENCH_APPEND_ROUNDS) \
		-warmup $(BENCH_APPEND_WARMUP) \
		-rows-per-round $(BENCH_APPEND_ROWS) \
		-batch-size $(BENCH_APPEND_BATCH) \
		-payload-size $(BENCH_APPEND_PAYLOAD) \
		-secondary-index=$(BENCH_APPEND_SECONDARY_INDEX) \
		-fail-ratio $(BENCH_APPEND_FAIL_RATIO)

bench-append-growth-single: bench-env-single
	$(MAKE) bench-append-growth \
		BENCH_APPEND_ADDR=127.0.0.1:5433

bench-append-growth-cluster: bench-env-cluster
	$(MAKE) bench-append-growth \
		BENCH_APPEND_ADDR=127.0.0.1:5433 \
		BENCH_APPEND_CLUSTER_ADDRS=127.0.0.1:5433,127.0.0.1:5434,127.0.0.1:5435

bench-append-growth-cluster-guardrail: bench-env-cluster
	$(MAKE) bench-append-growth \
		BENCH_APPEND_ADDR=127.0.0.1:5433 \
		BENCH_APPEND_CLUSTER_ADDRS=127.0.0.1:5433,127.0.0.1:5434,127.0.0.1:5435 \
		BENCH_APPEND_ROUNDS=8 \
		BENCH_APPEND_WARMUP=1 \
		BENCH_APPEND_ROWS=10000 \
		BENCH_APPEND_BATCH=250 \
		BENCH_APPEND_PAYLOAD=96 \
		BENCH_APPEND_SECONDARY_INDEX=true \
		BENCH_APPEND_FAIL_RATIO=1.25

bench-matrix-single:
	@echo "== single-node benchmark matrix =="
	@$(MAKE) bench-seed-single BENCH_SCENARIO=baseline BENCH_TX=$(BENCH_TX) BENCH_WORKERS=4 BENCH_STEPS=5 BENCH_MATERIALS=3 BENCH_INDEXES=true
	@$(MAKE) bench-seed-single BENCH_SCENARIO=wide-tx BENCH_TX=$(BENCH_TX) BENCH_WORKERS=4 BENCH_STEPS=20 BENCH_MATERIALS=10 BENCH_INDEXES=true
	@$(MAKE) bench-seed-single BENCH_SCENARIO=no-indexes BENCH_TX=$(BENCH_TX) BENCH_WORKERS=4 BENCH_STEPS=5 BENCH_MATERIALS=3 BENCH_INDEXES=false
	@$(MAKE) bench-seed-single BENCH_SCENARIO=high-workers BENCH_TX=$(BENCH_TX) BENCH_WORKERS=8 BENCH_STEPS=5 BENCH_MATERIALS=3 BENCH_INDEXES=true

bench-matrix-cluster:
	@echo "== cluster benchmark matrix =="
	@$(MAKE) bench-seed-cluster BENCH_SCENARIO=baseline BENCH_TX=$(BENCH_TX) BENCH_WORKERS=4 BENCH_STEPS=5 BENCH_MATERIALS=3 BENCH_INDEXES=true
	@$(MAKE) bench-seed-cluster BENCH_SCENARIO=wide-tx BENCH_TX=$(BENCH_TX) BENCH_WORKERS=4 BENCH_STEPS=20 BENCH_MATERIALS=10 BENCH_INDEXES=true
	@$(MAKE) bench-seed-cluster BENCH_SCENARIO=no-indexes BENCH_TX=$(BENCH_TX) BENCH_WORKERS=4 BENCH_STEPS=5 BENCH_MATERIALS=3 BENCH_INDEXES=false
	@$(MAKE) bench-seed-cluster BENCH_SCENARIO=high-workers BENCH_TX=$(BENCH_TX) BENCH_WORKERS=8 BENCH_STEPS=5 BENCH_MATERIALS=3 BENCH_INDEXES=true

# ── CI / Clean ───────────────────────────────────────────────────────────────

ci: tidy
	@files=$$(gofmt -l .); if [ -n "$$files" ]; then echo "Unformatted:"; echo "$$files"; exit 1; fi
	go vet ./...
	go test ./...
	go test -race ./...

clean:
	rm -rf .asql .asql-node-a .asql-node-b .asql-node-c .asql-node-d .demo-asql
