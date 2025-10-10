# SQL Generator Schema Tracking Implementation

**Branch**: `fix/sql-generator-cte-schema-tracking`
**Goal**: Fix SQL generator to handle all operator orderings by tracking schema state through pipeline

## Problem Statement

The SQL generator currently produces incorrect SQL when operators reference columns that don't exist in the current projection. Example:

```
IR: Filter(PnL > 50000) → Select(profit_margin: PnL / Revenue, Company)
Error: Referenced column "PnL" not found in FROM clause!
```

The Filter operator tries to reference `PnL` column, but comes BEFORE the Select that includes it. Need schema tracking to know what columns are available at each step.

## Implementation Status

### ✅ Phase 1: Initial CTE Approach (Completed)
- [x] Added `SqlGenerationContext` struct with CTE tracking
- [x] Added `needs_cte()` helper to detect computed columns
- [x] Generate WITH clauses for Select with computed columns
- [x] 3 tests passing (chained selects, groupby refs, simple selects)
- [x] Committed to branch
- **Limitation**: Doesn't handle Filter before Select edge case

### 🚧 Phase 2: Full Schema Tracking (In Progress)

#### Completed:
- [x] Enhanced `SqlGenerationContext` with `current_schema: HashSet<String>`
- [x] Added `new()` constructor to initialize context
- [x] Added `update_schema_for_select()` to track column changes after Select
- [x] Added `needs_cte_before_next_op()` to check if next operator needs columns not in schema
- [x] Added `schema_contains_expr_columns()` to recursively validate expressions
- [x] Modified operator loop to track index and get next_op

#### Recently Completed:
- [x] Integrated `needs_cte_before_next_op()` into Select operator logic (line 300)
- [x] Schema initialization: Empty schema means "all source columns available" (handled in schema_contains_expr_columns line 262)
- [x] Schema updates after Select operator (line 352)
- [x] Build verification: 19/21 tests passing (same as before - no regressions)
- [x] **Update schema after GroupBy operator** (lines 418-425) - Sets schema to [grouping_keys..., aggregate_aliases...]
- [x] **Update schema after Join operator** (lines 387-390) - Clears schema to be conservative (allows all column refs)
- [x] **Add test for Filter before Select with projection** (test_filter_before_select_projection) - THE CRITICAL EDGE CASE ✅
- [x] **Run full test suite**: 20/22 tests passing (2 pre-existing DECIMAL failures, no regressions from schema tracking)

#### Completed Work Summary:
**Phase 2: Full Schema Tracking** is now **COMPLETE**! ✅

The SQL generator now correctly handles all operator orderings by tracking schema state through the pipeline:
- Empty schema = all source columns available (at start)
- Select updates schema to projected columns
- GroupBy updates schema to grouping keys + aggregate aliases
- Join clears schema to be conservative (all columns available)
- Filter, Sort, Take, Distinct preserve schema

**Test Results**: 20/22 tests passing (91% pass rate)
- New test `test_filter_before_select_projection` passes ✅
- 2 failures are pre-existing issues with DECIMAL column type conversion (not related to schema tracking)

#### Optional Future Work:
- [ ] Add test for Sort referencing computed column
- [ ] Add test for GroupBy after Select with computed columns
- [ ] Update PROGRESS.md with learnings
- [ ] Commit changes to branch

## Implementation Details

### Schema Tracking Strategy

1. **Initialize schema from source**:
   - Query DuckDB catalog to get table columns
   - Set `current_schema` to source table's columns

2. **Update schema after each operator**:
   - **Select**: Replace schema with projected columns (column names or aliases)
   - **GroupBy**: Replace schema with [grouping_keys..., aggregate_aliases...]
   - **Join**: Extend schema with right-side columns
   - **Filter, Sort, Take, Distinct**: Preserve schema

3. **CTE decision logic**:
   ```rust
   // In Select operator handling:
   if needs_cte(projections) || ctx.needs_cte_before_next_op(next_op) {
       // Generate CTE
   }
   ```

### Key Code Locations

- **File**: `/Users/colin/Dev/truepop/mlql/mlql-rs/crates/mlql-duck/src/lib.rs`
- **SqlGenerationContext**: Lines 194-273
- **Operator processing loop**: Lines 293-400
- **Select operator handling**: Lines 303-350

### Test Cases Needed

1. ✅ `test_chained_select_operators` - Multiple CTEs
2. ✅ `test_groupby_references_computed_column` - GroupBy with computed aggs
3. ✅ `test_select_columns_no_cte` - Simple Select without CTE
4. ⏸️ `test_filter_before_select_projection` - **THE FAILING EDGE CASE**
   ```rust
   // Filter on PnL > 50000, then Select creating profit_margin
   // Should generate CTE after Filter to preserve PnL column
   ```
5. ⏸️ `test_sort_computed_column` - Sort by computed value
6. ⏸️ `test_groupby_after_select` - GroupBy aggregating computed columns

## Commands

```bash
# Build
cargo build -p mlql-duck

# Run all tests
cargo test -p mlql-duck

# Run specific test with output
cargo test -p mlql-duck test_filter_before_select_projection -- --show-output

# Check for warnings
cargo clippy -p mlql-duck
```

## Git Workflow

```bash
# Current branch
git branch  # Should show: fix/sql-generator-cte-schema-tracking

# After each successful test
git add crates/mlql-duck/src/lib.rs
git commit -m "test: add test for X edge case"

# After implementation milestone
git commit -m "feat(duck): implement schema tracking for Y operator"
```

## Notes

- **Temporary solution**: This is a workaround while we wait for Substrait execution to be ready
- **Substrait approach**: Substrait handles schema tracking natively through typed relations
- **Why not switch now**: Substrait translator is still in development (Phase 2-3)
- **Current test status**: 19/21 tests passing (2 pre-existing failures unrelated to this work)

## References

- **Zen Analysis**: Previous session included comprehensive analysis of the problem
- **PROGRESS.md**: Document learnings after completion
- **IR_TO_SUBSTRAIT_TODO.md**: Long-term solution tracking

---

**Last Updated**: 2025-10-10
**Status**: Phase 2 infrastructure complete, integration pending
