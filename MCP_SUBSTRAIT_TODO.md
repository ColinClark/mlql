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

### Phase 1: Analysis & Setup
- [ ] **Task 1**: Analyze current MCP server flow
  - [x] Read `main.rs`, `mcp.rs`, `query.rs`, `llm.rs`
  - [ ] Document current execution path
  - [ ] Identify integration points for Substrait

- [ ] **Task 2**: Design new execution path
  - [ ] Document Substrait execution flow
  - [ ] Plan SchemaProvider implementation
  - [ ] Plan error handling strategy

- [ ] **Task 3**: Add dependencies to mlql-server
  - [ ] Add `substrait = "0.61"` to Cargo.toml
  - [ ] Add `prost = "0.13"` to Cargo.toml
  - [ ] Verify builds: `cargo build -p mlql-server`
  - **Test**: Build succeeds
  - **Commit**: "feat(server): add substrait dependencies"

### Phase 2: Schema Provider
- [ ] **Task 4**: Create DuckDB catalog-based SchemaProvider
  - [ ] Create `crates/mlql-server/src/schema.rs`
  - [ ] Implement `DuckDbSchemaProvider` struct
  - [ ] Query DuckDB catalog for table schemas
  - [ ] Implement `SchemaProvider` trait
  - **Test**: Unit test schema lookup
  - **Commit**: "feat(server): implement DuckDB catalog SchemaProvider"

### Phase 3: Substrait Execution
- [ ] **Task 5**: Implement execute_ir_substrait() in query.rs
  - [ ] Create new function `execute_ir_substrait()`
  - [ ] Initialize SubstraitTranslator with DuckDbSchemaProvider
  - [ ] Translate MLQL IR → Substrait Plan
  - [ ] Serialize plan to bytes with prost
  - [ ] Execute via `SELECT * FROM from_substrait(?)`
  - **Test**: Simple query (table scan)
  - **Commit**: "feat(server): implement Substrait execution path"

- [ ] **Task 6**: Load Substrait extension when opening DuckDB
  - [ ] Add extension path configuration
  - [ ] Load extension in `DuckExecutor::open()`
  - [ ] Handle extension loading errors gracefully
  - **Test**: Verify extension loads
  - **Commit**: "feat(server): load Substrait extension on connection"

### Phase 4: Integration
- [ ] **Task 7**: Add execution mode configuration
  - [ ] Add `MLQL_EXECUTION_MODE` env var (sql/substrait)
  - [ ] Add `ExecutionMode` enum
  - [ ] Update query.rs to check mode
  - **Test**: Both modes work
  - **Commit**: "feat(server): add execution mode configuration"

- [ ] **Task 8**: Update MCP query handler to use Substrait path
  - [ ] Update `handle_query_tool()` in mcp.rs
  - [ ] Call `execute_ir_substrait()` when mode=substrait
  - [ ] Pass through same parameters (database path)
  - **Test**: MCP tool execution with Substrait
  - **Commit**: "feat(server): integrate Substrait execution in MCP handler"

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
