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

**Test**: ✅ `test_filter_with_comparison` passes - `from users | filter age > 18` → FilterRel
**Commit**: ✅ "feat(ir): implement Filter operator with column references"

**Schema Context Implementation**:
- Pipeline retrieves schema from source via `get_output_names()`
- Schema (Vec<String>) passed to `translate_operator()` and `translate_expr()`
- Column names resolve to field indices via `schema.iter().position()`
- Field references use Substrait StructField with index

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
**Commit**: Pending - "feat(ir): implement Take operator with DuckDB-compatible deprecated variants"
**Note**: Uses deprecated `OffsetMode::Offset(i64)` and `CountMode::Count(i64)` oneof variants because DuckDB v1.3 extension calls the deprecated `.offset()` and `.count()` accessor methods. The new `count_mode: CountExpr` API is not yet supported by DuckDB.

## Phase 3: Advanced Operators (Milestone 3)

### 3.1 Join Operator
- [ ] Translate `Operator::Join` → `JoinRel`
- [ ] Handle join types (Inner, Left, Right, Full)
- [ ] Translate join condition
- [ ] Handle right source (another table/subquery)

**Test**: `from users | join from orders on users.id == orders.user_id` → JoinRel
**Commit**: "feat(ir): translate join operator"

### 3.2 GroupBy/Aggregate Operator
- [ ] Translate `Operator::GroupBy` → `AggregateRel`
- [ ] Handle group keys
- [ ] Translate aggregate functions (sum, count, avg, min, max)
- [ ] Map MLQL aggs → Substrait agg functions

**Test**: `from sales | group by product { total: sum(amount) }` → AggregateRel
**Commit**: "feat(ir): translate groupby/aggregate operator"

### 3.3 Distinct Operator
- [ ] Translate `Operator::Distinct` → `AggregateRel` with no aggs
- [ ] Or use `DedupRel` if available

**Test**: `from users | distinct` → AggregateRel/DedupRel
**Commit**: "feat(ir): translate distinct operator"

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
