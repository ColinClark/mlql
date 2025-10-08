# MLQL IR → Substrait Translator Implementation Plan

**Created**: 2025-10-07
**Branch**: `feature/ir-to-substrait`
**Approach**: Direct Substrait protobuf generation using Rust `substrait` crate

## Why Direct Substrait?

We just built the DuckDB substrait extension to **consume** Substrait plans, not to use SQL as an intermediate. The goal is:
- MLQL IR → Substrait protobuf → DuckDB `from_substrait()` → Results
- Preserve MLQL semantics that may not map cleanly to SQL
- Full control over plan generation and optimization

## Phase 1: Foundation (Milestone 1)

### 1.1 Setup Module Structure ✅
- [x] Create `crates/mlql-ir/src/substrait/` directory
- [x] Add `mod.rs` with public API
- [x] Create `translator.rs` for core translation logic
- [x] Create `schema.rs` for schema/type mapping
- [x] Add substrait dependency to `mlql-ir/Cargo.toml`

**Test**: ✅ Module compiles with warnings (unused functions expected)
**Commit**: Pending - "feat(ir): add substrait module structure and schema provider"

### 1.2 Schema Provider Interface ✅
- [x] Define `SchemaProvider` trait for table metadata lookup
- [x] Implement mock provider for testing
- [x] Add method to resolve table → DuckDB schema
- [ ] Map MLQL types → Substrait types (deferred to Phase 2)

**Test**: ✅ Mock provider compiles, ready for use in tests
**Commit**: Combined with 1.1

### 1.3 Core Translator Scaffold ✅
- [x] Create `SubstraitTranslator` struct
- [x] Implement `translate(&Program) -> Result<Plan>` method
- [x] Build basic Plan structure (version, relations)
- [x] Add error types (Schema, UnsupportedOperator, Translation)
- [x] Scaffold pipeline and source translation methods

**Test**: ✅ Compiles, creates empty Plan with Substrait v0.53.0
**Commit**: Combined with 1.1 and 1.2

## Phase 2: Core Operators (Milestone 2)

### 2.1 Table Source (Read Relation) ✅
- [x] Translate `Source::Table` → `ReadRel`
- [x] Set base schema from provider
- [x] Handle table aliases (scaffolded, not used yet)
- [x] Generate `NamedTable` reference
- [x] Map common types (INTEGER, BIGINT, VARCHAR, FLOAT, DOUBLE)
- [x] Add unit test `test_simple_table_scan`
- [x] Add comprehensive test `test_substrait_plan_generation`
- [x] Fix missing `names` field in RelRoot (required for execution)
- [x] Add JSON serialization for debugging plan structure

**Test**: ✅ Both tests pass - plan generates correctly with proper schema and root names
**JSON Output**: Valid Substrait plan with version 0.53, ReadRel, and proper output column names
**Commit**: Pending - "feat(ir): translate table source to ReadRel with proper root names"

**Note**: Boolean type support skipped (substrait crate type structure unclear)
**BLOCKER**: DuckDB loadable extension hangs on macOS due to protobuf bug (https://github.com/protocolbuffers/protobuf/issues/4203)
  - Root cause: Recursive locking in GoogleOnceInitImpl during ParseFromString when called from dylib
  - Workaround: Use static extension for testing (works perfectly)
  - Long-term: Bug is macOS-specific, will work on Linux/Windows

### 2.2 Filter Operator ✅
- [x] Translate `Operator::Filter` → `FilterRel`
- [x] Convert `Expr` → Substrait `Expression`
- [x] Handle comparison ops (==, !=, <, >, <=, >=)
- [x] Handle logical ops (AND, OR, NOT)
- [x] Handle column references with schema context
- [x] Implement function extension system
- [x] Register functions with unique anchors
- [x] Generate extension URIs and declarations
- [x] Map operators to function signatures (e.g., "gt:i32_i32")

**Test**: ✅ `test_mlql_ir_to_substrait_execution` passes - `from users | filter age > 25` executes correctly!
**Commit**: ✅ 6cb4125 "feat(ir): implement Substrait function extension system for Filter operator"

**Schema Context Implementation**:
- Pipeline retrieves schema from source via `get_output_names()`
- Schema (Vec<String>) passed to `translate_operator()` and `translate_expr()`
- Column names resolve to field indices via `schema.iter().position()`
- Field references use Substrait StructField with index

**Function Extension System**:
- `FunctionRegistry` tracks used functions with HashMap (function_sig → anchor)
- `generate_extensions()` creates extension URIs and declarations in Plan
- Extension URI: `functions_comparison.yaml` from Substrait standard
- Function signatures: "function:arg_types" format (e.g., "gt:i32_i32", "not:bool")
- Each function gets unique anchor ID starting from 1
- Binary ops: register in `translate_binary_op` before creating ScalarFunction
- Unary ops: register in `translate_unary_op` before creating ScalarFunction

### 2.3 Project Operator (Select) ✅
- [x] Translate `Operator::Select` → `ProjectRel`
- [x] Handle wildcard `*` projection (via column references)
- [x] Handle specific column projections
- [x] Handle aliased expressions
- [x] Generate proper field references

**Test**: ✅ `test_select_specific_columns` passes - `from users | select [name, age]` → ProjectRel
**Commit**: ✅ "feat(ir): implement Select operator (ProjectRel)"

**Implementation Details**:
- ProjectRel wraps input relation with list of expressions
- Each Projection → Substrait Expression
- Column references resolve to FieldReference with correct indices
- Aliases handled via Projection::Aliased variant

### 2.4 Sort Operator ✅
- [x] Translate `Operator::Sort` → `SortRel`
- [x] Handle ascending/descending
- [x] Handle multiple sort keys
- [x] Map to Substrait sort direction (ASC_NULLS_FIRST=1, DESC_NULLS_LAST=4)
- [x] Add test `test_sort_with_multiple_keys`

**Test**: ✅ `from users | sort -age, +name` → SortRel (10 tests passing)
**Commit**: Pending - "feat(ir): implement Sort operator (SortRel)"

**Implementation Details**:
- SortRel wraps input relation with list of SortField
- Each SortKey → Substrait SortField with expression and direction
- Direction mapping: `desc=true` → 4 (DESC_NULLS_LAST), `desc=false` → 1 (ASC_NULLS_FIRST)
- Column references resolve to field indices via schema context
- Multiple sort keys supported (secondary sort handled correctly)

### 2.5 Take/Limit Operator ✅
- [x] Translate `Operator::Take` → `FetchRel`
- [x] Use deprecated oneof variants for DuckDB compatibility
- [x] Combine with existing plan
- [x] Add integration test test_take_limit
- [x] Fix API compatibility issue (use deprecated Offset/Count variants)

**Test**: ✅ `from users | take 2` → FetchRel (returns 2 rows correctly)
**Tests**: ✅ test_table_scan, test_take_limit, test_plan_generation all pass
**Commit**: ✅ dd08edf "feat(ir): implement Take operator and integration test suite"
**Note**: Uses deprecated `OffsetMode::Offset(i64)` and `CountMode::Count(i64)` oneof variants because DuckDB v1.3 extension calls the deprecated `.offset()` and `.count()` accessor methods. The new `count_mode: CountExpr` API is not yet supported by DuckDB.

**🎉 MILESTONE: Phase 2 Complete - All Core Operators Implemented!**
- ✅ Table source (ReadRel)
- ✅ Filter operator (FilterRel) - structure complete, needs function URIs
- ✅ Select operator (ProjectRel)
- ✅ Sort operator (SortRel)
- ✅ Take operator (FetchRel)
- ✅ Integration test suite with custom DuckDB build
- ✅ End-to-end execution: MLQL IR → Substrait → DuckDB → Results

## Phase 3: Advanced Operators (Milestone 3)

### 3.1 Join Operator
- [ ] Translate `Operator::Join` → `JoinRel`
- [ ] Handle join types (Inner, Left, Right, Full)
- [ ] Translate join condition
- [ ] Handle right source (another table/subquery)

**Test**: `from users | join from orders on users.id == orders.user_id` → JoinRel
**Commit**: "feat(ir): translate join operator"

### 3.2 GroupBy/Aggregate Operator ✅
- [x] Translate `Operator::GroupBy` → `AggregateRel`
- [x] Handle group keys with rootReference
- [x] Translate aggregate functions (sum implemented, others pending)
- [x] Map MLQL aggs → Substrait agg functions
- [x] Add projection to ReadRel for GroupBy
- [x] Calculate correct output schema for RelRoot
- [x] Implement `get_pipeline_output_names()` to track schema transformations

**Test**: ✅ `from sales | group by product { total: sum(amount) }` → AggregateRel (test_groupby passes)
**Commit**: Pending - "feat(ir): implement GroupBy operator with ReadRel projection and schema tracking"

**Implementation Details**:
- Uses AggregateRel with grouping keys and measures
- Projection in ReadRel filters columns to [grouping_keys... , aggregate_args...]
- Both grouping expressions and measures use rootReference
- RelRoot names calculated via `get_pipeline_output_names()` to reflect final schema
- Final output schema: [grouping_key_names... , aggregate_alias_names...]
- Function extension system registers aggregate functions (sum:i32)

### 3.3 Distinct Operator ✅
- [x] Translate `Operator::Distinct` → `AggregateRel` with no measures
- [x] Use deprecated `grouping_expressions` field for DuckDB compatibility
- [x] Group by all columns with rootReference field

**Test**: ✅ `from users | distinct` → AggregateRel (test_distinct passes)
**Commit**: ✅ 29de2a1 "feat(ir): implement Distinct operator (AggregateRel)"

**Implementation Details**:
- Uses AggregateRel with grouping on all columns, no measures (standard Substrait pattern)
- DuckDB v1.4.0 requires deprecated `grouping_expressions` field inside Grouping message
- Field references include `rootReference` to match DuckDB's format
- Test verifies deduplication of exact duplicate rows (5 input rows → 3 distinct rows)

## Phase 4: Integration & Testing (Milestone 4)

### 4.1 End-to-End Test with DuckDB
- [ ] Create integration test module
- [ ] Load substrait extension in test
- [ ] Translate MLQL IR → Substrait
- [ ] Execute with `from_substrait()`
- [ ] Compare results with expected output

**Test**: Full pipeline execution through DuckDB
**Commit**: "test(ir): add end-to-end substrait integration tests"

### 4.2 Error Handling
- [ ] Add proper error types
- [ ] Handle unsupported operators gracefully
- [ ] Provide helpful error messages
- [ ] Add validation before translation

**Test**: Verify error messages for unsupported features
**Commit**: "feat(ir): improve error handling"

### 4.3 Documentation
- [ ] Document `SubstraitTranslator` API
- [ ] Add examples to module docs
- [ ] Document schema provider requirements
- [ ] Add operator mapping reference

**Commit**: "docs(ir): document substrait translator"

## Phase 5: Advanced Features (Future)

### 5.1 Window Functions
- [ ] Translate `Operator::Window` → `WindowRel`
- [ ] Handle partition by, order by
- [ ] Map window functions

### 5.2 Set Operations
- [ ] Translate `Union`, `Except`, `Intersect` → SetRel
- [ ] Handle UNION ALL vs UNION

### 5.3 Subqueries
- [ ] Handle `Source::SubPipeline`
- [ ] Nest substrait plans
- [ ] Support correlated subqueries if needed

## Testing Strategy

**Per-feature tests:**
1. Unit test: MLQL operator → Substrait relation
2. Snapshot test: Verify protobuf structure
3. Integration test: Execute via DuckDB, verify results

**Test data:**
- Use in-memory DuckDB with sample tables
- Simple test cases (users, orders, products)
- Complex multi-operator pipelines

**Commit after each successful test phase**

## Dependencies

```toml
[dependencies]
substrait = "0.61"  # Already added
prost = "0.13"       # For protobuf
```

## Success Criteria

**Milestone 1**: Table source translates, test passes
**Milestone 2**: Basic pipeline (table → filter → project → take) works end-to-end
**Milestone 3**: Complex query (join + groupby) executes correctly
**Milestone 4**: All core operators tested, documented

## Notes

- Start simple: get ONE operator working end-to-end first
- Test frequently: every operator should have passing tests before commit
- Use DuckDB's `get_substrait()` output as reference for structure
- Schema provider can query DuckDB catalog at runtime
