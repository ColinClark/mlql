# MCP Server Substrait Integration

**Branch**: `mcp-mlql-substrait`
**Goal**: Replace SQL-based execution with Substrait-based execution in MCP server

## Current Architecture

```
Natural Language → OpenAI → MLQL IR → SQL → DuckDB → Results
```

**Implementation**:
- `llm.rs`: Converts natural language to MLQL IR using OpenAI
- `query.rs`: `execute_ir()` generates SQL from IR using `mlql-duck`
- `mcp.rs`: MCP protocol handler that calls `execute_ir()`

## Target Architecture

```
Natural Language → OpenAI → MLQL IR → Substrait Plan → DuckDB (from_substrait) → Results
```

**New Implementation**:
- Keep `llm.rs` unchanged (NL → MLQL IR works perfectly)
- Add `execute_ir_substrait()` in `query.rs` using `mlql-ir/substrait`
- Load Substrait extension when opening DuckDB connection
- Update MCP handler to use Substrait path

## Tasks

### Phase 1: Analysis & Setup ✅
- [x] **Task 1**: Analyze current MCP server flow
  - [x] Read `main.rs`, `mcp.rs`, `query.rs`, `llm.rs`
  - [x] Document current execution path
  - [x] Identify integration points for Substrait
  - **Commit**: 37fb6ee "docs(server): add comprehensive documentation"

- [x] **Task 2**: Design new execution path
  - [x] Document Substrait execution flow
  - [x] Plan SchemaProvider implementation
  - [x] Plan error handling strategy
  - **Commit**: 37fb6ee "docs(server): add comprehensive documentation"

- [x] **Task 3**: Add dependencies to mlql-server
  - [x] Add `substrait = "0.61"` to Cargo.toml
  - [x] Add `prost = "0.14"` to Cargo.toml (fixed version)
  - [x] Verify builds: `cargo build -p mlql-server`
  - **Test**: Build succeeds ✅
  - **Commit**: dd58408 "feat(server): add substrait and prost dependencies"

### Phase 2: Schema Provider ✅
- [x] **Task 4**: Create DuckDB catalog-based SchemaProvider
  - [x] Use existing `crates/mlql-server/src/catalog.rs`
  - [x] Implement `DuckDbSchemaProvider` struct
  - [x] Query DuckDB catalog for table schemas
  - [x] Implement `SchemaProvider` trait
  - **Test**: Compiles ✅
  - **Commit**: a7bb5d4 "feat(server): implement DuckDB SchemaProvider"

### Phase 3: Substrait Execution ✅
- [x] **Task 5**: Implement execute_ir_substrait() in query.rs
  - [x] Create new function `execute_ir_substrait()`
  - [x] Initialize SubstraitTranslator with DuckDbSchemaProvider
  - [x] Translate MLQL IR → Substrait Plan
  - [x] Serialize plan to bytes with prost
  - [x] Execute via `SELECT * FROM from_substrait(?)`
  - **Test**: Compiles ✅
  - **Commit**: 908293d "feat(server): implement Substrait execution path"

- [x] **Task 6**: Load Substrait extension when opening DuckDB
  - [x] Add extension path configuration (optional)
  - [x] Load extension if SUBSTRAIT_EXTENSION_PATH is set
  - [x] Handle extension loading errors gracefully
  - **Test**: Build succeeds ✅
  - **Commit**: 908293d (combined with Task 5)

### Phase 4: Integration ✅
- [x] **Task 7**: Add execution mode configuration
  - [x] Add `MLQL_EXECUTION_MODE` env var (sql/substrait)
  - [x] Add `ExecutionMode` enum
  - [x] Implement `execute_ir_auto()` dispatcher
  - **Test**: Build succeeds ✅
  - **Commit**: 1534fe6 "feat(server): add execution mode configuration"

- [x] **Task 8**: Update MCP query handler to use Substrait path
  - [x] Update `handle_query_tool()` in mcp.rs
  - [x] Call `execute_ir_auto()` to dispatch based on mode
  - [x] Update response format to show execution info
  - **Test**: Build succeeds ✅
  - **Commit**: 77e30bd "feat(server): integrate Substrait execution in MCP handler"

### Phase 5: Testing & Polish
- [ ] **Task 9**: End-to-end testing
  - [ ] Test: Table scan query
  - [ ] Test: Filter query
  - [ ] Test: Join query
  - [ ] Test: GroupBy with aggregates
  - [ ] Test: Complex multi-operator pipeline
  - **Test**: All operator types work
  - **Commit**: "test(server): add Substrait execution integration tests"

- [ ] **Task 10**: Error handling and fallback
  - [ ] Add try-catch around Substrait execution
  - [ ] Log Substrait errors clearly
  - [ ] Optional: Fallback to SQL on Substrait failure
  - **Test**: Error messages are helpful
  - **Commit**: "feat(server): add error handling for Substrait execution"

- [ ] **Task 11**: Update documentation
  - [ ] Update `crates/mlql-server/README.md`
  - [ ] Document MLQL_EXECUTION_MODE configuration
  - [ ] Document Substrait extension requirement
  - [ ] Add examples of both execution modes
  - **Test**: Documentation is clear
  - **Commit**: "docs(server): document Substrait execution mode"

- [ ] **Task 12**: Test with demo.duckdb database
  - [ ] Run server with data/demo.duckdb
  - [ ] Execute various queries via MCP
  - [ ] Compare results with SQL mode
  - [ ] Verify performance and correctness
  - **Test**: Real-world database works
  - **Commit**: "test(server): verify Substrait execution with demo database"

## Key Files

### To Modify
- `crates/mlql-server/Cargo.toml` - Add dependencies
- `crates/mlql-server/src/query.rs` - Add execute_ir_substrait()
- `crates/mlql-server/src/mcp.rs` - Update handler to use Substrait
- `crates/mlql-server/README.md` - Document new mode

### To Create
- `crates/mlql-server/src/schema.rs` - DuckDB catalog SchemaProvider

### Reference (No Changes)
- `crates/mlql-ir/src/substrait/translator.rs` - Substrait translator
- `crates/mlql-ir/src/substrait/schema.rs` - SchemaProvider trait
- `crates/mlql-server/src/llm.rs` - NL → IR conversion (unchanged)

## Testing Strategy

**After each task**:
1. Build: `cargo build -p mlql-server`
2. Test: `cargo test -p mlql-server`
3. Manual test: Run server and verify functionality
4. Commit: Descriptive message with passing tests

**Final integration test**:
```bash
# Terminal 1: Start server with Substrait mode
MLQL_EXECUTION_MODE=substrait cargo run -p mlql-server

# Terminal 2: Test queries
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"query": "Show me all users over age 25"}'
```

## Success Criteria

- [ ] Server can execute queries via Substrait path
- [ ] All MLQL operators work (Filter, Select, Join, GroupBy, etc.)
- [ ] Results match SQL-based execution
- [ ] Error handling is robust
- [ ] Documentation is complete
- [ ] Both SQL and Substrait modes are supported

## Notes

- **Extension Loading**: Need to ensure Substrait extension is loaded before executing queries
- **Schema Discovery**: SchemaProvider must query DuckDB catalog at runtime
- **Backwards Compatibility**: Keep SQL mode working for comparison/fallback
- **Performance**: Substrait may be faster for complex queries (no SQL parsing overhead)

---

**Created**: 2025-10-08
**Status**: Phase 1 - Planning
