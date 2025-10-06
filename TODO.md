# MLQL Development TODO

## Current Sprint: Complete Core Operators

### Phase 1: Expression Support ✅ COMPLETED
- [x] Column references
- [x] Literals (int, float, string, bool, null)
- [x] Binary operators (arithmetic, comparison, logical)
- [x] Function calls

### Phase 2: Basic Operators (Filter, Sort, Take)
- [x] Filter operator (WHERE clause)
- [x] Sort operator (ORDER BY with ASC/DESC)
- [x] Take operator (LIMIT)
- [x] Select with wildcard (*)

### Phase 3: Projection Operator
**Status:** ✅ COMPLETED

**Tasks:**
- [x] Test select with specific columns: `from users | select [name, age]` ✅
- [x] Test select with expressions: `from users | select [age * 2 as double_age]` ✅
- [ ] Test select with functions: `from users | select [upper(name) as NAME]`
- [ ] Test mixed projections: `from users | select [id, upper(name) as name, age + 1 as next_age]`

**Fix Applied:** Rewrote Pest grammar to capture operators as separate rules (`add_op`, `mul_op`) instead of inline patterns. Parser now correctly builds BinaryOp AST nodes.

**Test Cases to Write:**
```mlql
// Specific columns
from users | select [name, age]

// With expressions
from users | select [age * 2 as double_age, name]

// With functions
from users | select [upper(name) as NAME, age]

// Complex mix
from users | select [id, upper(name) as name, age + 1 as next_age]
```

### Phase 4: Filter Combinations
**Status:** ✅ COMPLETED

**Tasks:**
- [x] Test simple comparisons: `filter age > 25` ✅
- [x] Test AND conditions: `filter age > 25 && age < 40` ✅
- [x] Test OR conditions: `filter name == "Alice" || name == "Bob"` ✅
- [x] Test LIKE operator: `filter name like "A%"` ✅

**Tests Added:**
- `test_filter_simple_comparison` - WHERE (age > 25)
- `test_filter_and_condition` - WHERE ((age > 25) AND (age < 40))
- `test_filter_or_condition` - WHERE ((name = 'Alice') OR (name = 'Bob'))
- `test_filter_like_operator` - WHERE (name LIKE 'A%')

All tests passing with correct SQL generation!

### Phase 5: Distinct Operator
**Status:** ✅ COMPLETED

**Tasks:**
- [x] Implement DISTINCT in IR-to-SQL ✅
- [x] Test: `from users | select [city] | distinct` ✅
- [x] Test with multiple columns: `from locations | select [city, state] | distinct` ✅

**Tests Added:**
- `test_distinct_single_column` - SELECT DISTINCT city FROM users
- `test_distinct_multiple_columns` - SELECT DISTINCT city, state FROM locations

**SQL Generated:**
```sql
SELECT DISTINCT city FROM users
SELECT DISTINCT city, state FROM locations
```

All tests passing!

### Phase 6: GroupBy Operator
**Status:** ✅ COMPLETED

**Tasks:**
- [x] Implement GROUP BY in IR-to-SQL ✅
- [x] Support aggregate functions (sum, count, avg, min, max) ✅
- [x] Test simple groupby with count(*) ✅
- [x] Test with multiple aggregates (sum, avg) ✅

**Tests Added (Both Passing):**
- `test_group_by_simple` - SELECT city, count(*) AS total FROM orders GROUP BY city
- `test_group_by_multiple_aggregates` - SELECT product, sum(qty) AS total_qty, avg(price) AS avg_price FROM sales GROUP BY product

**Implementation Details:**
- GROUP BY keys become first columns in SELECT
- Aggregate functions with aliases appended
- Supports count(*), sum(col), avg(col), etc.
- Uses JSON IR (parser support for GROUP BY syntax not yet implemented)

**SQL Generated:**
```sql
SELECT city, count(*) AS total FROM orders GROUP BY city
SELECT product, sum(qty) AS total_qty, avg(price) AS avg_price FROM sales GROUP BY product
```

All tests passing!

### Phase 7: Join Operator
**Status:** ✅ COMPLETED

**Tasks:**
- [x] Implement JOIN in IR-to-SQL (INNER, LEFT, RIGHT, FULL, CROSS) ✅
- [x] Test INNER JOIN with ON condition ✅
- [x] Test LEFT JOIN ✅
- [x] Test multiple joins (3 tables) ✅

**Tests Added (All Passing):**
- `test_join_inner` - SELECT * FROM users INNER JOIN orders ON condition
- `test_join_left` - SELECT * FROM users LEFT JOIN orders ON condition
- `test_join_multiple` - Chained JOINs across 3 tables (users → orders → products)

**Implementation Details:**
- JOIN modifies FROM clause instead of adding WHERE conditions
- Supports INNER, LEFT, RIGHT, FULL OUTER, CROSS JOIN types
- Multiple JOINs chain in FROM clause
- ON conditions use expr_to_sql()
- SEMI/ANTI joins not yet supported

**SQL Generated:**
```sql
SELECT * FROM users AS u INNER JOIN orders AS o ON (u.id = o.user_id)
SELECT * FROM users AS u LEFT JOIN orders AS o ON (u.id = o.user_id)
SELECT * FROM users AS u INNER JOIN orders AS o ON (u.id = o.user_id) INNER JOIN products AS p ON (o.product_id = p.id)
```

All tests passing!

### Phase 8: Union/Except/Intersect
**Status:** 📋 Planned

**Tasks:**
- [ ] Implement UNION/UNION ALL
- [ ] Implement EXCEPT
- [ ] Implement INTERSECT
- [ ] Test set operations

**Test Cases to Write:**
```mlql
// Union
from current_users | union | from archived_users

// Union all
from sales_2023 | union all | from sales_2024

// Except
from all_users | except | from banned_users

// Intersect
from premium_users | intersect | from active_users
```

## Phase 9: Error Handling & Edge Cases
**Status:** 📋 Planned

**Tasks:**
- [ ] Test empty result sets
- [ ] Test invalid column references
- [ ] Test type mismatches
- [ ] Test SQL injection attempts (should be safe with parameterization)
- [ ] Test budget limits (max_rows, max_memory, timeout)

## Phase 10: Documentation & Examples
**Status:** 📋 Planned

**Tasks:**
- [ ] Add inline documentation to IR-to-SQL functions
- [ ] Create examples directory with sample queries
- [ ] Document SQL generation strategy
- [ ] Add troubleshooting guide

## Future Enhancements (Post-MVP)
- [ ] Window functions
- [ ] Subqueries in FROM clause
- [ ] CTEs (WITH clause)
- [ ] Vector search (KNN)
- [ ] Time-series resampling
- [ ] Graph traversal operators
- [ ] HTTP API server
- [ ] Query plan caching
- [ ] Policy enforcement (PII masking, row-level security)

---

## Development Workflow

1. Pick a task from current phase
2. Write test case first (TDD)
3. Implement IR-to-SQL translation
4. Run tests until passing
5. Commit with descriptive message
6. Update this TODO.md (mark task as complete)
7. Move to next task

## Test Command
```bash
# Run all tests
cargo test

# Run specific crate tests
cargo test -p mlql-duck

# Run with output
cargo test -p mlql-duck -- --show-output

# Run specific test
cargo test -p mlql-duck test_select_specific_columns -- --show-output
```

## Progress Tracking
- ✅ = Completed
- 🚧 = In Progress
- 📋 = Planned
- ❌ = Blocked
