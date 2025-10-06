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
**Status:** 📋 Planned

**Tasks:**
- [ ] Test simple comparisons: `filter age > 25`
- [ ] Test AND conditions: `filter age > 25 && name == "Alice"`
- [ ] Test OR conditions: `filter age < 20 || age > 60`
- [ ] Test LIKE operator: `filter name like "A%"`

**Test Cases to Write:**
```mlql
// Simple comparison
from users | filter age > 25

// AND condition
from users | filter age > 25 && age < 40

// OR condition
from users | filter name == "Alice" || name == "Bob"

// LIKE pattern
from users | filter name like "A%"
```

### Phase 5: Distinct Operator
**Status:** 📋 Planned

**Tasks:**
- [ ] Implement DISTINCT in IR-to-SQL
- [ ] Test: `from users | select [name] | distinct`
- [ ] Test with multiple columns: `from users | select [name, age] | distinct`

**Test Cases to Write:**
```mlql
// Single column distinct
from users | select [name] | distinct

// Multi-column distinct
from users | select [city, state] | distinct
```

### Phase 6: GroupBy Operator
**Status:** 📋 Planned

**Tasks:**
- [ ] Implement GROUP BY in IR-to-SQL
- [ ] Support aggregate functions (sum, count, avg, min, max)
- [ ] Test simple groupby: `group by city { total: count(*) }`
- [ ] Test with multiple aggregates
- [ ] Test with having clause (if supported in IR)

**Test Cases to Write:**
```mlql
// Simple count
from users | group by city { total: count(*) }

// Multiple aggregates
from users | group by city {
  total: count(*),
  avg_age: avg(age),
  max_age: max(age)
}

// With filter after group
from sales | group by product { revenue: sum(price * qty) } | filter revenue > 1000
```

### Phase 7: Join Operator
**Status:** 📋 Planned

**Tasks:**
- [ ] Implement JOIN in IR-to-SQL (INNER, LEFT, RIGHT, FULL)
- [ ] Test simple join: `join from orders on users.id == orders.user_id`
- [ ] Test with join type: `join from orders on users.id == orders.user_id type: left`
- [ ] Test multiple joins (chained)

**Test Cases to Write:**
```mlql
// Inner join
from users | join from orders on users.id == orders.user_id

// Left join
from users | join from orders on users.id == orders.user_id type: left

// Multi-table join
from users
| join from orders on users.id == orders.user_id
| join from products on orders.product_id == products.id
```

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
