package ironflow

import (
	"fmt"
	"math/rand/v2"
	"net"
	"sync/atomic"
	"testing"
)

// The kernel hands its ephemeral ports to every bind on port 0 and every
// outgoing connection, so a port picked there and released before its server
// binds it can go to another socket in the gap (#4052). The kernel never hands
// out a port below the range.
const (
	freePortFirst = 20000
	freePortCount = 32768 - freePortFirst
)

var nextFreePort atomic.Uint32

func init() { nextFreePort.Store(rand.Uint32N(freePortCount)) }

func freePort(t *testing.T) int {
	t.Helper()
	for range freePortCount {
		port := freePortFirst + int(nextFreePort.Add(1)%freePortCount)
		l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err == nil {
			_ = l.Close()
			return port
		}
	}
	t.Fatalf("no free port in %d-%d", freePortFirst, freePortFirst+freePortCount-1)
	return 0
}
