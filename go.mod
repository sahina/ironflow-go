// Engine-internal module path. Public consumers import the mirror path
// github.com/sahina/ironflow-go/ironflow — this divergence is intentional;
// see docs/adr/0022-go-mirror-import-rewrite-carve-out.md.
module github.com/sahina/ironflow-go

go 1.26.0

require (
	connectrpc.com/connect v1.21.0
	github.com/gorilla/websocket v1.5.3
	go.opentelemetry.io/contrib/bridges/otelslog v0.21.0
	go.opentelemetry.io/otel v1.47.0
	go.opentelemetry.io/otel/log v1.47.0
	go.opentelemetry.io/otel/sdk v1.47.0
	go.opentelemetry.io/otel/sdk/log v1.47.0
	go.opentelemetry.io/otel/trace v1.47.0
	golang.org/x/sync v0.24.0
	google.golang.org/protobuf v1.36.12
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/go-logr/logr v1.4.4 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/google/uuid v1.6.0 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/otel/metric v1.47.0 // indirect
	golang.org/x/sys v0.49.0 // indirect
)

require (
	golang.org/x/net v0.62.0
	golang.org/x/text v0.43.0 // indirect
)
