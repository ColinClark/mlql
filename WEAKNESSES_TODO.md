# MLQL System Weaknesses - TODO List

**Created**: 2025-10-10
**Status**: In Progress

## Overview

This document tracks identified weaknesses in the MLQL system and their resolution status.

---

## ✅ Weakness 0: Cross-Table Query Recognition (LLM Prompt)

**Status**: COMPLETE ✅
**Commit**: c438b4d

**Problem**: LLM not recognizing when queries require JOINs between tables
- Example: "companies similar to banks that failed" → generated Filter instead of Join

**Solution**: Enhanced LLM prompt with explicit cross-table query recognition section
- Added examples of cross-table query patterns
- Listed keywords indicating cross-table relationships
- Warning against using simple filters for cross-table comparisons

---

## ❌ Weakness 1: Cross-Table Joins (LLM Semantic Understanding)

**Status**: TODO
**Priority**: HIGH

**Problem**: LLM generates JOIN operations but chooses wrong join columns
- Example: "show me countries and their companies" → joined Country = Company (meaningless)
- Example: "join bank failures and companies" → joined Bank = Company (no matches in data)

**Root Causes**:
1. No schema relationship metadata (foreign keys)
2. LLM guessing join conditions semantically
3. No validation of join results

**Test Cases**:
- [ ] Join on wrong columns returns 0 rows → should suggest no natural relationship
- [ ] Explicit join condition: "join users and orders on user_id"
- [ ] Natural join: "join users and orders" (if FK exists)

**Implementation Plan**:
1. Add schema metadata support (foreign key relationships)
2. Enhance LLM prompt with join examples and strategies
3. Add join validation (warn when 0 rows returned)
4. Support explicit join conditions in natural language

**Files to Modify**:
- `crates/mlql-server/src/llm.rs` - LLM prompt enhancement
- `crates/mlql-duck/src/schema.rs` - Schema metadata (NEW)
- `crates/mlql-ir/src/types.rs` - Schema relationship types (NEW)

---

## ❌ Weakness 2: Date/Time Handling

**Status**: TODO
**Priority**: HIGH

**Problem**: Date columns return NULL in results despite working in SQL
- All date values serialize as `null` in JSON output
- Catalog shows dates as `<unsupported>`
- Date range queries convert to exact dates: "from 2023" → "= 2023-01-01"

**Root Causes**:
1. Result serialization doesn't handle date types
2. Arrow RecordBatch → JSON conversion loses date values
3. LLM converts date ranges to exact comparisons

**Test Cases**:
- [ ] SELECT date column → returns actual date values (not null)
- [ ] "from 2023" → generates >= 2023-01-01 AND < 2024-01-01
- [ ] "between 2020 and 2023" → proper range
- [ ] "last 30 days" → relative date computation
- [ ] "year(date) = 2023" → date extraction functions

**Implementation Plan**:
1. Fix date serialization in `to_json_rows()`
2. Add date range support to IR (DateRange expression type)
3. Enhance LLM prompt with date examples
4. Support date extraction functions (year, month, day)
5. Support relative dates ("last N days", "this year")

**Files to Modify**:
- `crates/mlql-duck/src/lib.rs` - `to_json_rows()` date handling
- `crates/mlql-ir/src/types.rs` - DateRange expression type
- `crates/mlql-server/src/llm.rs` - Date query examples

---

## ❌ Weakness 3: Window Functions / Ranking

**Status**: TODO
**Priority**: MEDIUM

**Problem**: No window function support
- "rank by PnL" → tries to compute `1 + PnL` as rank column
- "number companies 1-8" → references non-existent "row_number" column
- Operation order error: Sort before Select causes column unavailability

**Root Causes**:
1. No Window operator in IR
2. LLM doesn't understand window function syntax
3. No OVER clause support

**Test Cases**:
- [ ] "rank companies by revenue" → ROW_NUMBER() OVER (ORDER BY revenue DESC)
- [ ] "top 3 per state" → PARTITION BY state
- [ ] "running total of sales" → SUM() OVER (ORDER BY date)
- [ ] "difference from previous month" → LAG() function

**Implementation Plan**:
1. Add Window operator to IR:
   ```json
   {
     "op": "Window",
     "functions": [{
       "func": "row_number",
       "alias": "rank",
       "over": {
         "partition_by": ["category"],
         "order_by": [{"column": "revenue", "desc": true}]
       }
     }]
   }
   ```
2. Implement Window → SQL translation in mlql-duck
3. Support functions: ROW_NUMBER, RANK, DENSE_RANK, NTILE, LAG, LEAD, SUM OVER
4. Enhance LLM prompt with ranking examples

**Files to Modify**:
- `crates/mlql-ir/src/types.rs` - Window operator and types
- `crates/mlql-duck/src/lib.rs` - Window → SQL translation
- `crates/mlql-server/src/llm.rs` - Window function examples

---

## ❌ Weakness 4: Subqueries

**Status**: TODO
**Priority**: MEDIUM

**Problem**: Cannot perform nested queries
- "revenue above average" → generates `Revenue > (Revenue >= 0)` (nonsensical)
- No scalar subquery support
- No IN/EXISTS support

**Root Causes**:
1. No Subquery expression type in IR
2. LLM doesn't understand subquery patterns
3. No correlated subquery support

**Test Cases**:
- [ ] "above average revenue" → WHERE revenue > (SELECT AVG(revenue) FROM companies)
- [ ] "users with orders" → WHERE id IN (SELECT user_id FROM orders)
- [ ] "countries with GDP > their continent average" → correlated subquery
- [ ] "users without orders" → NOT EXISTS

**Implementation Plan**:
1. Add Subquery expression type to IR:
   ```json
   {
     "type": "Subquery",
     "query": {
       "source": {...},
       "ops": [...]
     },
     "scalar": true
   }
   ```
2. Support in Filter conditions
3. Support IN/NOT IN operators
4. Support EXISTS/NOT EXISTS
5. Enhance LLM prompt with subquery examples

**Files to Modify**:
- `crates/mlql-ir/src/types.rs` - Subquery expression type
- `crates/mlql-duck/src/lib.rs` - Subquery → SQL translation
- `crates/mlql-server/src/llm.rs` - Subquery examples

---

## ❌ Weakness 5: Common Table Expressions (CTEs)

**Status**: TODO
**Priority**: LOW

**Problem**: Cannot build multi-step queries with CTEs
- "first get top 3 countries, then show density" → tries to join with wrong table
- Chained operations don't make semantic sense

**Root Causes**:
1. No CTE support in IR (only single pipeline)
2. LLM tries to use JOINs instead of CTEs

**Test Cases**:
- [ ] "top 3 countries by GDP, then their population" → CTE
- [ ] "filter users, then aggregate" → CTE for clarity
- [ ] Multiple CTEs chained together

**Implementation Plan**:
1. Add CTE support to IR:
   ```json
   {
     "ctes": [
       {
         "name": "top_countries",
         "query": {"source": {...}, "ops": [...]}
       }
     ],
     "source": {"type": "Table", "name": "top_countries"},
     "ops": [...]
   }
   ```
2. Translate to WITH clauses in SQL
3. Enhance LLM prompt with multi-step examples

**Files to Modify**:
- `crates/mlql-ir/src/types.rs` - CTE support in Program
- `crates/mlql-duck/src/lib.rs` - WITH clause generation
- `crates/mlql-server/src/llm.rs` - Multi-step query examples

---

## ✅ Weakness 6: HAVING Clause

**Status**: COMPLETE ✅ (Already Working)

**Verification**: Query "show states with more than 20 bank failures" works correctly
- Generates proper HAVING clause
- Returns correct results

No action needed.

---

## ❌ Weakness 7: CASE/WHEN Expressions

**Status**: TODO (Analysis Incomplete)
**Priority**: MEDIUM

**Problem**: [Details not provided in input]

**Test Cases**: TBD

**Implementation Plan**: TBD

---

## Testing Strategy

For each weakness:
1. Write failing test case demonstrating the issue
2. Implement the feature
3. Verify test passes
4. Run full test suite (no regressions)
5. Commit with descriptive message
6. Update this document with ✅

---

## Progress Tracking

- Total Weaknesses: 7
- Complete: 2 ✅
- In Progress: 0
- TODO: 5 ❌

**Current Focus**: TBD

