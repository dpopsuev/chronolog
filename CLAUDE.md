# Chronolog

Log consolidation MCP server. Many logs in, one clean signal out.

## Architecture

Hexagonal architecture:
- `internal/domain/` — domain types (Event, Edge, cascade hierarchy, annotations)
- `internal/port/` — store interfaces (7 ISP interfaces composed into Store)
- `internal/store/` — adapters: SQLiteStore (prod), MemStore (testkit)
- `internal/parser/` — timestamp detection and parsing
- `internal/mcp/` — MCP server with 6 tools
- `internal/config/` — YAML config resolution
- `internal/cli/` — Cobra CLI (serve, version)
- `cmd/chronolog/` — entry point

## MCP Tools

6 tools: `chronolog` (cascade lifecycle), `intake` (receiving dock), `graph` (stage+merge), `query` (timeline+FTS5+trace), `diff` (hot-cold), `projection` (tensor).

## Cascade Hierarchy

Domain → Environment → Session → Instance → Phase → Event. Domain is tree root.

## Build

```sh
make preflight   # fmt + vet + lint + test
make build       # produces ./chronolog binary
make install-hooks  # pre-commit hook
```

## Conventions

- All entity IDs are UUIDs. Named aliases are mutable via MetaStore.
- Use `slog` for all logging. stdlib `log` is forbidden (enforced by depguard).
- One log line = one event. No multi-line grouping.
- Store interface in `internal/port/`. Adapters in `internal/store/`.
- MCP is the primary interface. CLI is an afterthought.
- Git semantics: intake = working directory, stage = git add, merge = git commit.
- `.chronolog` file = portable SQLite database (investigation).
