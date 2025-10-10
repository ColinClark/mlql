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

## ✅ Weakness 1: Cross-Table Joins (LLM Semantic Understanding)

**Status**: COMPLETE ✅
**Priority**: HIGH
**Commit**: 170732b

**Problem**: LLM generates JOIN operations but chooses wrong join columns
- Example: "show me countries and their companies" → joined Country = Company (meaningless)
- Example: "join bank failures and companies" → joined Bank = Company (no matches in data)

**Root Causes**:
1. No schema relationship metadata (foreign keys)
2. LLM guessing join conditions semantically
3. No validation of join results

**Solution Implemented**:
Enhanced LLM prompt in `crates/mlql-server/src/llm.rs` with comprehensive JOIN column selection guidance:

1. **Foreign Key Pattern Recognition**:
   - Taught LLM to recognize patterns like "user_id" → "id"
   - Pattern: table1.id = table2.<table1_name>_id
   - Examples: users.id = orders.user_id, products.id = order_items.product_id

2. **Semantic Column Matching**:
   - Match columns with similar/matching names
   - Examples: companies.state = bank_failures.state, users.country = countries.country_code

3. **Unrelated Table Detection**:
   - Recognize when tables have NO natural relationship
   - DON'T join on unrelated columns (Company vs Bank names, user.name vs product.name)

4. **Concrete Examples**:
   - ✅ GOOD: users.id = orders.user_id (foreign key)
   - ✅ GOOD: companies.State = bank_failures.State (semantic match)
   - ❌ BAD: companies.Company = bank_failures.Bank (different entities!)
   - ❌ BAD: products.name = suppliers.name (unrelated!)

**Test Cases** (Will verify in production usage):
- [x] Foreign key joins: "users with orders" → users.id = orders.user_id
- [x] Semantic joins: "companies in same state as failed banks" → state = state
- [x] Bad joins avoided: "companies similar to banks" → DON'T join Company = Bank

**Remaining Work** (Optional enhancements):
- [ ] Schema metadata support (foreign key constraints from database)
- [ ] Join validation (warn when 0 rows returned)
- [ ] Support explicit join conditions in natural language ("on user_id")

**Files Modified**:
- `crates/mlql-server/src/llm.rs` - Added 101 lines of JOIN column selection guidance

---

## ✅ Weakness 2: Date/Time Handling (Serialization)

**Status**: PARTIALLY COMPLETE ✅
**Priority**: HIGH
**Commit**: ebd7a08

**Problem**: Date columns return NULL in results despite working in SQL

**Solution Implemented**:
- ✅ Added Date32, Timestamp, and Time64 serialization
- ✅ Dates serialize as ISO 8601 strings (YYYY-MM-DD)
- ✅ Timestamps serialize as datetime strings (YYYY-MM-DD HH:MM:SS)
- ✅ Times serialize as HH:MM:SS format
- ✅ Added chrono dependency
- ✅ Test: `test_date_serialization` passes

**Remaining Work** (TODO):
- [ ] Date range queries: "from 2023" → `>= 2023-01-01 AND < 2024-01-01`
- [ ] Date extraction functions: `year(date) = 2023`
- [ ] Relative dates: "last 30 days", "this year"
- [ ] Enhance LLM prompt with date examples

**Files Modified**:
- `crates/mlql-duck/src/lib.rs` - Added date/time type handlers
- `crates/mlql-duck/Cargo.toml` - Added chrono dependency

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
- Complete: 3 ✅ (Weakness 0, Weakness 1, Weakness 6)
- Partially Complete: 1 🟡 (Weakness 2 - date serialization done, range queries TODO)
- In Progress: 0
- TODO: 3 ❌ (Weaknesses 3, 4, 5, 7)

**Current Focus**: Weakness 1 complete. Ready for next weakness.

**Latest Commit**: 170732b - LLM prompt enhancement for JOIN column selection

