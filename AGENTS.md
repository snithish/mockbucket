# AGENTS.md - Guide for AI Coding Agents

MockBucket is a standalone object-storage emulator (S3, STS, GCS) built in Go 1.26+.

## Build / Lint / Test Commands

```sh
make build          # go build -o ./bin/mockbucketd ./cmd/mockbucketd
make run            # ./bin/mockbucketd --config mockbucket.yaml
make test           # go test ./...
make fmt            # gofmt -w $(git ls-files '*.go')
make tidy           # go mod tidy
make compat         # scripts/compat/run_all.sh (requires running server)
make clean          # rm -rf ./bin
```

Run a single package: `go test ./internal/iam`
Run a single test: `go test ./internal/iam -run TestEvaluatorHonorsExplicitDeny`
Run a subtest: `go test ./internal/server -run TestS3FrontendContract/BucketLevelAPI`
Verbose: `go test -v ./internal/server -run TestSTSAssumeRoleAndSessionCanHeadBucket`

**Always run before finishing:** `gofmt -w $(git ls-files '*.go') && go test ./...`

## Python

All Python work in this project uses `uv`. Never install packages globally or with `pip`/`pip3` directly.
Use `uv run` to execute scripts and `uv pip` to manage dependencies within the project's virtual environment.

```sh
uv run scripts/compat/run_all.py serve          # start server for manual testing
uv run scripts/compat/run_all.py test           # run all cloud tests (default)
uv run scripts/compat/run_all.py test aws       # run AWS S3/STS tests only
uv run scripts/compat/run_all.py test gcs       # run GCS tests only
uv run scripts/compat/run_all.py --debug test   # verbose HTTP logging
```

## Architecture

- Single daemon `mockbucketd`. Object bytes on filesystem, metadata in SQLite.
- `internal/storage` - persistence (SQLiteStore, FilesystemObjectStore).
- `internal/iam` - policy evaluation, trust, sessions.
- `internal/auth/<provider>` - request authentication, SigV4, bearer tokens.
- `internal/frontends/<provider>` - wire protocol translation (S3, STS, GCS, Azure).
- `internal/httpx` - shared middleware, error mapping, request context.
- `internal/core` - sentinel errors, domain models.
- `internal/config` - YAML config loading and validation.
- `internal/seed` - YAML seed data parsing and application.
- `internal/server` - HTTP server bootstrap, health endpoints.
- Seed YAML drives startup state. No runtime admin API in v1.
- S3 and GCS are mutually exclusive at runtime. STS requires S3.
- Azure is a disabled scaffold.

## Code Style

### Imports
- Alias cross-provider imports: `authaws ".../auth/aws"`, `mbconfig ".../config"`.
- Group stdlib, external, then internal imports separated by blank lines.

### Naming
- Exported: PascalCase (`SQLiteStore`, `PolicyDocument`).
- Unexported: camelCase (`withTx`, `initSchema`, `escapeLike`).
- Acronyms stay uppercase: `SQL`, `XML`, `ETag`, `ARN`, `CRC32C`, `S3`, `GCS`, `STS`.

### Error Handling
- Sentinel errors in `internal/core/errors.go` using `errors.New()`.
- Wrap with `fmt.Errorf("context: %w", err)`.
- Map errors to HTTP via `httpx.StatusCode()`.
- Discard explicitly: `_ = runtime.Close()`, `_, _ = w.Write(...)`.

### HTTP Handlers
- Signature: `func handleXxx(w http.ResponseWriter, r *http.Request, deps common.Dependencies, ...)`.
- Early return on error: `writeError(w, err); return`.
- XML responses: `writeXML(w, status, payload)`. JSON: `writeJSON(w, status, payload)`.
- Auth check: `allow(r, deps, action, resource)`.

### Struct Tags
- Config: `yaml:"field_name"` (snake_case).
- Domain: `json:"field" yaml:"field"`.
- XML responses: `xml:"ElementName"`.
- Permissions: `0o755`, `0o644` (octal with `0o` prefix).

### Testing
- Stdlib `testing` only. No testify or gomega.
- Fatal format: `t.Fatalf("FunctionName() error = %v", err)` or `t.Fatalf("field = %v, want %v", got, want)`.
- Helpers use `t.Helper()`. Temp dirs via `t.TempDir()`. Cleanup via `t.Cleanup()`.
- Table-driven tests with anonymous struct slices.
- Subtests: `t.Run("name", func(t *testing.T) { ... })`.
- HTTP unit tests: `httptest.NewRequest` + `httptest.NewRecorder`.
- Integration tests: `httptest.NewServer` + real AWS/GCS SDK.
- Test seed data as `const` strings in test files.
- Protocol tests MUST use real AWS SDK, not custom signing helpers.

### Logging
- `log/slog` (stdlib structured logging): `slog.String("key", v)`, `slog.Any("error", err)`.

### General
- ASCII only unless file requires otherwise.
- Sparse comments - only where code is non-obvious.
- Streaming object I/O - never buffer full objects in memory.
- Keep `go.mod`/`go.sum` tidy via `go mod tidy`.

## Constraints
- Keep diffs minimal and atomic per phase.
- Every behavior change lands with focused tests.
- Do not expose GCS/Azure endpoints before their enablement phases.
- Do not move provider wire logic into `storage` or `iam`.
- Explicit deny always wins in policy evaluation.
- Do not revert changes you did not make.
