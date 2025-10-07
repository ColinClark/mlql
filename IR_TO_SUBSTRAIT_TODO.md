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

**Test**: ✅ Both tests pass - plan generates correctly with proper schema
**Commit**: Pending - "feat(ir): translate table source to ReadRel"

**Note**: Boolean type support skipped (substrait crate type structure unclear)
**Note**: DuckDB execution testing deferred - loadable extension has initialization issues, will address in substrait project

### 2.2 Filter Operator
- [ ] Translate `Operator::Filter` → `FilterRel`
- [ ] Convert `Expr` → Substrait `Expression`
- [ ] Handle comparison ops (==, !=, <, >, <=, >=)
- [ ] Handle logical ops (AND, OR, NOT)
- [ ] Handle column references

**Test**: `from users | filter age > 18` → FilterRel
**Commit**: "feat(ir): translate filter operator"

### 2.3 Project Operator (Select)
- [ ] Translate `Operator::Select` → `ProjectRel`
- [ ] Handle wildcard `*` projection
- [ ] Handle specific column projections
- [ ] Handle aliased expressions
- [ ] Generate proper field references

**Test**: `from users | select [name, age]` → ProjectRel
**Commit**: "feat(ir): translate select/project operator"

### 2.4 Sort Operator
- [ ] Translate `Operator::Sort` → `SortRel`
- [ ] Handle ascending/descending
- [ ] Handle multiple sort keys
- [ ] Map to Substrait sort direction

**Test**: `from users | sort -age, +name` → SortRel
**Commit**: "feat(ir): translate sort operator"

### 2.5 Take/Limit Operator
- [ ] Translate `Operator::Take` → `FetchRel`
- [ ] Set limit value
- [ ] Combine with existing plan

**Test**: `from users | take 10` → FetchRel
**Commit**: "feat(ir): translate take/limit operator"

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
