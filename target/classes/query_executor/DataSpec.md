# Data Structure Specification (`Data`)

This document defines the runtime `Data` structure used by query-executor nodes.

## 1) Purpose

`Data` is the shared in-memory working set passed across data-query nodes.

It must support:
- representing tabular data consistently (including single-value outputs)
- filtering/selection
- row-wise and column-wise calculations
- CRUD precondition validation
- tracking enough metadata to map results back to storage locations

## 2) Scope Boundary

`Data` owns:
- runtime tabular state
- shape/location validation
- filtering and projection helpers
- operation helpers on selected values

`Data` does not own:
- query-tree control flow
- parse-tree structure rules
- storage I/O implementation (CRUD engine does that)

## 3) Core Model

`Data` should represent one object at a time in the first version.

Recommended internal model:
- `objectName`
- ordered `rowIds` for current working set
- ordered column map: `columnName -> ColumnVector`
- per-column metadata:
  - attribute type
  - origin (`STORED` or `DERIVED`)
  - optional source descriptor
- cell storage keyed by row position (or row id)
- optional location map to storage coordinates for stored cells

### 3.1 Row Identity

Use stable row identity during execution:
- `rowId` should not change when filtering narrows the set
- filtering should create a new active subset, not renumber source identity

### 3.2 Value Representation

Each cell should keep:
- typed value (or null)
- origin marker (stored/derived)
- optional storage location (for stored values)

This is required for update/delete location validation.

## 4) Shape Definitions

These shapes are semantic, not separate types.

- `TABULAR`: any valid row/column subset
- `FULL_ROW_SET`: every column present for selected rows
- `FULL_COLUMN_SET`: full column(s) across selected rows
- `SINGLE_COLUMN`: one column across one or more rows
- `SINGLE_VALUE`: one scalar result, still representable in tabular form
- `EXISTING_LOCATIONS`: selected target cells map to real storage coordinates

## 5) Required Validation API

`Data` should expose validations used by CRUD nodes.

Minimum validation methods:
- `boolean isReadable()`
- `boolean isCreatableAsRows()`
- `boolean isCreatableAsColumns()`
- `boolean isUpdatable()`
- `boolean isDeletableAsRows()`
- `boolean isDeletableAsColumns()`
- `boolean hasExistingLocations()`
- `boolean isSingleColumn()`
- `boolean isSingleValue()`
- `boolean isFullRowSet()`
- `boolean isFullColumnSet()`

Optional richer variant:
- `ValidationResult validateCreate(...)`
- `ValidationResult validateRead(...)`
- `ValidationResult validateUpdate(...)`
- `ValidationResult validateDelete(...)`

Where `ValidationResult` includes:
- pass/fail
- machine code enum
- human-readable reason

## 6) Filtering API

Filtering must be deterministic and composable.

Minimum methods:
- `Data selectColumns(List<String> columnNames)`
- `Data filterRows(RowPredicate predicate)`
- `Data applySelection(List<String> columnNames, RowPredicate predicate)`

`RowPredicate` should evaluate against a row view with typed values.

Filtering rules:
- preserve row identity
- preserve column metadata for retained columns
- preserve storage locations for retained stored cells
- return a new `Data` view or copy (choose one and keep consistent)

## 7) Operation API

### 7.1 Row-wise Operations

Minimum methods:
- `Data applyRowOperation(RowOperationKind op, List<String> sourceColumns, String outputColumnName)`

Behavior:
- compute value per row from source columns
- output a single derived column
- preserve row identity

### 7.2 Column-wise Operations

Minimum methods:
- `Data applyColumnOperation(ColumnOperationKind op, String sourceColumn, String outputValueName)`

Behavior:
- compute scalar from one selected column
- return scalar in tabular-compatible form

### 7.3 Binary-input Operations

For operations requiring two upstream selections:
- the node resolves two `Data` inputs
- `Data` must provide merge/alignment helpers before operation

Minimum helpers:
- `Data alignByRowId(Data other)`
- `Data mergeColumnsFrom(Data other)`

## 8) CRUD Integration Contract

Node responsibilities:
- select/operate to prepare `Data`
- call `Data` validations
- delegate final storage mutation/read to CRUD layer

`Data` responsibilities:
- tell whether shape/location constraints are satisfied
- provide target coordinates/values for storage layer calls

Minimum export helpers:
- `List<RowMutation> toRowMutations(...)`
- `List<CellMutation> toCellMutations(...)`
- `List<RowLocation> targetRowLocations()`
- `List<ColumnLocation> targetColumnLocations()`

## 9) Null and Missing Semantics

Must distinguish:
- column missing from working set
- cell present with null value
- cell present but not mapped to existing storage location

These are different states and must not collapse.

## 10) Error Model

`Data` should fail with explicit error codes for:
- missing column
- type mismatch
- invalid operation input shape
- non-existent update/delete location
- illegal create/delete shape

Prefer typed exception + code enum over plain strings.

## 11) Determinism Rules

To keep behavior predictable:
- keep insertion order for columns
- keep stable row ordering for a given filtered set
- avoid implicit widening/narrowing of selection scope
- never auto-create storage locations during validation

## 12) Performance Baseline (First Version)

First version priorities:
- correctness
- clear invariants
- readable code

Acceptable initial tradeoffs:
- in-memory copies over complex lazy views
- linear scans over indexing inside `Data`

Optimize after behavior is stable.

## 13) Recommended First Implementation Checklist

1. Implement core state containers in `Data`.
2. Implement read-only introspection helpers (row/column counts, has column, etc.).
3. Implement selection/filter operations.
4. Implement row-wise and column-wise operations.
5. Implement shape validations.
6. Implement location validations.
7. Add exhaustive unit tests for validation truth table.
8. Integrate one CRUD node end-to-end (`DataReadNode` first).

## 14) Test Matrix Requirements

At minimum, test:
- one row, one column
- many rows, one column
- one row, many columns
- many rows, many columns
- null values
- derived-only columns
- mixed stored+derived columns
- two-selection merge and alignment
- each CRUD validation success/failure path

## 15) Open Decisions To Freeze Before Coding

- copy-on-write vs mutable-in-place `Data`
- row-id type (`int`, `long`, UUID, etc.)
- exact representation for scalar-as-tabular
- merge policy for dual-child operations:
  - strict row-id intersection
  - left-preserving join
  - explicit strategy enum
