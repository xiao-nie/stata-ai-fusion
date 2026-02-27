# Changelog

## 0.2.2 (2026-02-27)

### Security
- **Codebook command injection prevention**: Variable names are now validated against a strict regex before being embedded in Stata commands
- **Graph export path safety**: Uses `cd` + relative filename instead of embedding the full tmpdir path in Stata commands; auto-generated filenames use UUID instead of timestamps

### Bug Fixes
- **Windows StataBE-64 detection**: Added `statabe-64` to edition map so Windows BE edition is correctly identified (was falling back to IC)
- **search_log avoids unnecessary session creation**: Uses `get_session()` instead of `get_or_create()` to prevent spinning up a new Stata process just to search an empty log
- **Output preservation in `_clean_do_output`**: Refined `. ` stripping to only remove echoed command lines (starting with letters/underscores), preserving numeric output that happens to start with `. `
- **Batch return code captured**: `run_do_file` now reports non-zero Stata exit codes instead of silently ignoring them
- **`width` parameter validation**: `export_graph` rejects width values outside 100–10000 to prevent resource exhaustion
- **`timeout` parameter clamped**: `run_command` and `run_do_file` clamp timeout to 1–3600s to prevent negative or unbounded values

### Internal
- Removed dead `register()` functions and unused `Server` imports from all 9 tool modules
- Removed unused `_EDITION_PRIORITY` constant and empty `TYPE_CHECKING` block
- Added `py.typed` marker for PEP 561 type checker support
- Log buffer capped at 1000 entries (FIFO eviction) to bound memory usage
- Temp `.do` file cleanup failures now logged at debug level

## 0.2.1 (2026-02-27)

### Bug Fixes
- **Process group kill in `close()`**: Session cleanup now kills the entire Stata process group (including MP worker processes) instead of just the main process
- **Batch mode process isolation**: `_run_batch()` uses `start_new_session=True` for proper process group isolation; timeout kills the whole group
- **Stale log cleanup**: `run_do_file` removes leftover `.log` files before each batch run to prevent reading stale output
- **Multi-graph capture**: `maybe_inject_graph_export()` now injects `graph export` after *every* graph command (not just the last), with unique timestamps
- **Unused imports removed**: Cleaned up `base64` and `uuid` imports in `run_do_file.py`
- **Tool description guidance**: `run_command` description now directs AI to use `run_do_file` for long-running models (mixed, bootstrap, simulate)
- **Session timeout auto-cleanup**: `SessionManager` tracks per-session activity and automatically closes idle sessions after 1 hour (configurable via `session_timeout`)

### Internal
- MCP Server now exposes 11 tools (added `stata_cancel_command`)
- Batch do-file execution rewritten with `subprocess.Popen` for reliability
- Improved timeout handling with partial output capture

## 0.2.0 (2026-02-19)

### Features
- Session stability improvements
- Cancel command tool (`stata_cancel_command`)
- Batch-mode do-file execution

## 0.1.0 (2025-02-19)

### Features
- MCP Server with 10 tools (run_command, inspect_data, get_results, etc.)
- Skill knowledge base (5,600+ lines, 14 reference documents)
- VS Code extension with syntax highlighting, snippets, and code execution
- Auto-discovery of Stata installation (macOS/Linux/Windows)
- Multi-session support with data isolation
- Graph caching and auto-export
- r()/e()/c() result extraction
- MCP Resources for Skill knowledge access

### Supported Stata Versions
- Stata 17, 18, 19 (MP, SE, IC, BE)
- StataNow editions
