# Query Language Design

This note is intentionally not a SQL proposal.

The goal is to design a language native to this project, while still respecting how the current executor works.

## 1. Constraint

The current executor already has a working runtime shape:

- it consumes indexed AST text
- `IndexedAstParser` turns that text into `AstDocument`
- `IndexedAstBinder` binds that document into executable nodes
- the node tree executes itself

So the language layer should do this:

`source language -> lexer -> parser -> semantic AST -> lowered executor AST text`

The source language can be original. The executor contract should stay explicit.

## 2. What The Executor Actually Understands

Current executor node families:

- object statements
- attribute statements
- data statements
- selections
- joins
- row operations
- column operations

Current executor gap:

- there is no node that creates new rows from inline literal values

That gap matters because row creation is a real primitive, not a corner case.

## 3. Immediate Executor Addition

The executor should gain one new source node for literal row creation.

Suggested node:

```text
LiteralRows
- objectName
- columnNames
- rows
```

Suggested executor behavior:

- execute to a `Data` value shaped as creatable rows
- feed directly into `DataCreate`

That would let the language express “make these new rows” directly, without pretending those rows came from a prior selection.

Suggested corresponding additions:

- new `AstNodeType`: `LiteralRows`
- binder support in `IndexedAstBinder`
- `DataCreateNode` child support for `LiteralRows`

This is the smallest executor change that makes row creation first-class.

## 4. Language Design Principle

Do not start from SQL words.

Start from semantic primitives.

The language needs a way to express:

- object operations
- attribute operations
- reading data
- selecting rows
- choosing columns
- combining relations
- making derived row values
- reducing columns to scalars
- creating rows
- updating existing cells
- deleting rows or attributes

That is the real language core.

Syntax comes after that.

## 5. Proposed Semantic AST

This is the AST the parser should produce before lowering.

```text
Script
└── List<Statement>

Statement
├── ObjectStatement
├── AttributeStatement
├── ReadStatement
├── CreateRowsStatement
├── CreateColumnStatement
├── UpdateStatement
└── DeleteStatement
```

### 5.1 Object / attribute statements

```text
ObjectStatement
- kind: CREATE | READ | RENAME | DELETE
- objectName
- parentObjectName?
- newObjectName?
- includeAttributeMetadata?
- includeRowCount?

AttributeStatement
- kind: CREATE | READ | RENAME | DELETE
- objectName
- attributeName
- attributeType?
- newAttributeName?
- includeAttributeType?
```

### 5.2 Data statements

```text
ReadStatement
- source: RelationExpr

CreateRowsStatement
- objectName
- rows: List<LiteralRow>

CreateColumnStatement
- objectName
- columnName
- valueExpr: RowExpr

UpdateStatement
- objectName
- target: SelectionExpr
- assignments: List<Assignment>

DeleteStatement
- objectName
- mode: DELETE_ROWS | DELETE_COLUMNS
- target: SelectionExpr | ColumnSet
```

### 5.3 Relation and value expressions

```text
RelationExpr
├── SelectionExpr
├── JoinExpr
├── RowExpr
├── ColumnExpr
└── LiteralRowsExpr

SelectionExpr
- objectName
- columns: List<ColumnRef>
- predicate?: PredicateExpr

JoinExpr
- left: RelationExpr
- right: RelationExpr
- joinKind
- leftColumn
- rightColumn

RowExpr
- operator
- inputs: List<ValueExpr>
- outputName

ColumnExpr
- operator
- input: ValueExpr
- outputName

LiteralRowsExpr
- objectName
- columnNames
- rows

ValueExpr
├── ColumnRef
├── Literal
├── RowExpr
└── ColumnExpr

PredicateExpr
├── ComparisonExpr
├── BooleanExpr
└── NotExpr
```

## 6. AST Rules Worth Keeping

These rules matter more than concrete syntax.

### 6.1 Reads produce data

Every read-like construct should lower to a `DataRead` root.

### 6.2 Row creation is explicit

Creating rows should not be disguised as a read or a copy.

It should have its own statement in the surface AST and lower to:

```text
DataCreate
└── LiteralRows
```

### 6.3 Derived columns are distinct from new rows

These are different semantic actions:

- new rows extend row count
- new derived columns extend column set

That distinction should stay visible in the AST and in the language.

### 6.4 Filters belong to selection

Predicates should attach to selection-like nodes, not float around separately.

### 6.5 Lowering handles qualification

The source language can allow simple names.

The lowerer should decide when names must become fully qualified executor names like:

- `people.age`
- `pets.owner_name`

## 7. Lowering Targets

The semantic AST should lower into the executor AST the code already knows.

### 7.1 Existing lowering targets

Use current executor nodes for:

- object statements
- attribute statements
- filtered selection
- join
- row operation
- column operation
- update
- delete

### 7.2 New lowering target

Add one new lowering target:

```text
LiteralRows
```

That is the missing bridge for row insertion / row creation.

## 8. Language Design Questions

These are the real design questions to answer before picking keywords or punctuation.

### 8.1 Is the language sentence-like or symbolic?

Possible directions:

- command-like
- algebraic
- pipe-based
- indentation-based
- bracketed / structural

### 8.2 Does selection read as “take from object” or “shape an object”?

Possible mental models:

- retrieval
- transformation
- construction

### 8.3 Are joins explicit graph links or plain combinations?

You may want a language that emphasizes:

- connection
- matching
- merging

rather than SQL’s table-product heritage.

### 8.4 Are derived values written as formulas, functions, or node-like forms?

All three are viable:

- infix: `age + bonus`
- prefix: `add(age, bonus)`
- structural: `add [age bonus]`

### 8.5 Should row creation use tuple syntax?

It does not have to.

Possible representations:

- ordered tuples
- named maps
- block forms

Named maps are often better for a custom language because they are self-describing.

## 9. Recommended Direction

Start by designing the language around these semantic categories:

- `object`
- `attribute`
- `read`
- `where`
- `join`
- `derive`
- `make rows`
- `change`
- `drop`

But do not commit yet to SQL-like words such as:

- `select`
- `insert`
- `update`
- `delete`

unless you actually want them.

The AST does not require SQL vocabulary.

## 10. Lexer / Parser Plan

The implementation plan should still be conventional even if the language is original.

### 10.1 Lexer

The lexer should only know:

- identifiers
- literals
- operators
- delimiters
- reserved words

It should not know AST meaning.

### 10.2 Parser

The parser should produce the semantic AST, not executor nodes.

That means:

- parse statements
- parse value expressions
- parse predicates
- parse row literals

### 10.3 Lowerer

The lowerer should:

- allocate executor node ids
- build `AstDocument`
- emit indexed AST text
- reject source constructs not representable in the executor

## 11. Smallest Working Vertical Slice

The first complete path should be:

1. one native read statement
2. one predicate form
3. one row-creation statement

Specifically:

- a read that lowers to `DataRead -> Selection`
- a create-rows statement that lowers to `DataCreate -> LiteralRows`

That gives the language both:

- observation
- construction

which is a much better foundation than read-only syntax.

## 12. Practical Next Step

Before drafting final syntax, define these enums and node families in the semantic AST:

- statement kinds
- relation-expression kinds
- value-expression kinds
- predicate kinds
- literal row representation

Then choose a surface syntax that feels like your language rather than inherited SQL.

The AST should lead. The spelling should follow.
