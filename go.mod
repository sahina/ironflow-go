// Engine-internal module path. Public consumers import the mirror path
// github.com/sahina/ironflow-go/ironflow — this divergence is intentional;
// see docs/adr/0022-go-mirror-import-rewrite-carve-out.md.
module github.com/sahina/ironflow-go

go 1.25.0

require (
	connectrpc.com/connect v1.19.2
	github.com/gorilla/websocket v1.5.3
	golang.org/x/net v0.54.0
	golang.org/x/sync v0.20.0
	google.golang.org/protobuf v1.36.11
)

require golang.org/x/text v0.37.0 // indirect
