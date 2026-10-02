package ironflow

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

// #2472: a 403 from an environment/key-scope mismatch used to be answered with
// the API-key hint, which sends the user to the wrong setting.
func TestAuthError_403(t *testing.T) {
	const body = `{"code":"forbidden","message":"environment does not match API key scope"}`
	cases := []struct {
		name       string
		env        string
		wantEnv    bool
		wantServer bool
	}{
		{"environment header sent", "env_other", true, true},
		{"no environment header", "", false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusForbidden)
				_, _ = w.Write([]byte(body))
			}))
			defer srv.Close()

			headers := map[string]string{}
			if tc.env != "" {
				headers[HeaderEnvironment] = tc.env
			}
			fns := map[string]Function{"f": {}}
			err := registerFunctions(context.Background(), srv.URL, headers, fns, srv.Client(), NewNoopLogger())

			if !errors.Is(err, ErrForbidden) {
				t.Fatalf("want ErrForbidden, got %v", err)
			}
			msg := err.Error()
			if !strings.Contains(msg, "environment does not match API key scope") {
				t.Errorf("server message missing: %s", msg)
			}
			if !strings.Contains(msg, "IRONFLOW_API_KEY") {
				t.Errorf("API-key hint dropped: %s", msg)
			}
			hasEnv := strings.Contains(msg, `"env_other"`) && strings.Contains(msg, "WorkerConfig.Environment") && strings.Contains(msg, "IRONFLOW_ENV")
			if hasEnv != tc.wantEnv {
				t.Errorf("environment hint present=%v, want %v: %s", hasEnv, tc.wantEnv, msg)
			}
		})
	}
}

func TestAuthError_401Unchanged(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"message":"bad key"}`))
	}))
	defer srv.Close()

	err := registerFunctions(context.Background(), srv.URL, map[string]string{HeaderEnvironment: "env_x"},
		map[string]Function{"f": {}}, srv.Client(), NewNoopLogger())
	if !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("want ErrUnauthorized, got %v", err)
	}
	if strings.Contains(err.Error(), "WorkerConfig.Environment") || strings.Contains(err.Error(), "bad key") {
		t.Errorf("401 must keep the API-key hint only: %v", err)
	}
}

type envHintLogger struct {
	Logger
	errors chan string
}

func (l envHintLogger) Error(msg string, args ...any) { l.errors <- msg }

// The stream itself 403s after registration succeeds: the log line must name
// the environment, not only the API key (#2472).
func TestStreamingWorker_403NamesEnvironment(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "WorkerService/Connect") {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"code":"permission_denied","message":"environment does not match API key scope"}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	})
	srv := httptest.NewUnstartedServer(h2c.NewHandler(mux, &http2.Server{}))
	srv.Start()
	defer srv.Close()

	logs := envHintLogger{Logger: NewNoopLogger(), errors: make(chan string, 8)}
	fn := CreateFunction(FunctionConfig{ID: "fn"}, func(ctx Context) (any, error) { return nil, nil })
	w := NewStreamingWorker(WorkerConfig{
		ServerURL:   srv.URL,
		Environment: "env_other",
		Functions:   []Function{fn},
		Logger:      logs,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := w.Run(ctx); err == nil {
		t.Fatal("want an auth error")
	}
	msg := <-logs.errors
	if !strings.Contains(msg, `"env_other"`) || !strings.Contains(msg, "IRONFLOW_API_KEY") {
		t.Errorf("want environment and API-key hints, got: %s", msg)
	}
}
