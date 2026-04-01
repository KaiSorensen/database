# Relational Database Engine in Java and C#

A miniature in-memory relational database engine, implemented twice: once in Java and once in C#.

The purpose of this project is not merely to build a toy SQL interpreter. It is to understand, from first principles, how a relational database works: how a query becomes syntax, then meaning, then an execution plan, then data movement.

This is a systems project, a language project, and a thinking project.

---

## Why this exists

I want to understand something foundational.

Relational databases quietly underlie much of modern civilization. They sit beneath finance, logistics, manufacturing, healthcare, software infrastructure, internal tools, analytics, and operational decision-making. They are one of the most important examples of how abstract mathematics becomes concrete engineering.

This project is a way to study that deeply.

By building a small relational database engine from scratch, I expect to internalize:

- the essence of SQL as a language of transformations over relations
- the distinction between syntax, semantics, and execution
- the architecture of a real system with meaningful abstractions
- how performance emerges from representation and data movement
- how scalability begins long before distribution
- how design choices propagate through an entire system

This will also serve as a direct comparison between Java and C# as languages for building nontrivial systems.

---

## Project goals

The engine is intentionally scoped around the core of a relational database.

The goals are:

- implement a small but serious SQL subset
- parse SQL into a structured representation
- model schemas, tables, rows, and expressions
- plan and execute relational queries
- support filtering, projection, sorting, joins, grouping, and aggregation
- optionally support simple indexing
- keep the engine fully in memory
- write the system twice, once in Java and once in C#

This project is not trying to become a production database. It is trying to become a clean, understandable nucleus.

---

## Scope

### In scope

- `CREATE TABLE`
- `INSERT`
- `SELECT`
- `WHERE`
- `ORDER BY`
- `LIMIT`
- `INNER JOIN`
- `GROUP BY`
- aggregates such as `COUNT`, `SUM`, `AVG`
- basic scalar types
- a REPL or command interface
- execution plan inspection via a simple `EXPLAIN`
- optional single-column indexes

### Out of scope

At least initially, this project will not attempt:

- distributed execution
- replication
- consensus
- durability / WAL / recovery
- full transaction isolation
- MVCC
- cost-based optimization
- full SQL compatibility
- production-grade storage
- networked clustering

Those are all important, but they are downstream of the core educational objective.

---

## Core idea

A relational database is more than a parser and more than a bag of tables.

A query goes through several conceptual stages:

1. **Lexing**  
   Raw SQL text is turned into tokens.

2. **Parsing**  
   Tokens are turned into an AST or equivalent structured query representation.

3. **Semantic interpretation**  
   Names are resolved, schemas are consulted, expressions are typed, and the query is understood as an operation over relations.

4. **Planning**  
   The logical query is converted into an executable plan.

5. **Execution**  
   Operators run over in-memory data structures and produce results.

That pipeline is the heart of the project.

---

## Proposed architecture

The project will likely be structured around these modules:

### 1. Lexer
Responsible for tokenizing SQL input.

Questions it answers:
- What is an identifier?
- What is a keyword?
- What is a literal?
- Where should syntax errors be detected?

### 2. Parser
Responsible for turning tokens into structured query objects.

Questions it answers:
- What is the grammar of the SQL subset?
- How are expressions represented?
- How should syntax errors be surfaced?

### 3. AST / Query Model
Represents statements and expressions.

Likely concepts:
- statements
- table references
- column references
- predicates
- literals
- binary operators
- aggregate expressions
- orderings

### 4. Catalog / Schema
Tracks database metadata.

Likely concepts:
- tables
- columns
- column types
- indexes

### 5. Storage Layer
In-memory representation of tables and rows.

Questions it answers:
- How should rows be represented?
- How should nulls be handled?
- What tradeoffs exist between simplicity and performance?

### 6. Planner
Turns logical queries into execution plans.

Possible plan nodes:
- table scan
- filter
- projection
- sort
- limit
- join
- aggregate

### 7. Executor
Runs physical plan nodes and produces result rows.

This is where relational semantics become machinery.

### 8. Interface Layer
A REPL, CLI, test harness, or later a minimal client/server wrapper.

---

## Suggested folder structure

Keep both implementations structurally identical and only as detailed as the engine currently needs.

```text
/
├── README.md
├── docs/
│   ├── architecture/
│   └── notes/
├── examples/
│   └── queries/
├── java/
│   ├── src/
│   │   ├── ast/
│   │   ├── catalog/
│   │   ├── common/
│   │   ├── executor/
│   │   ├── lexer/
│   │   ├── parser/
│   │   ├── planner/
│   │   ├── repl/
│   │   └── storage/
│   ├── tests/
│   └── scripts/
└── csharp/
    ├── src/
    │   ├── ast/
    │   ├── catalog/
    │   ├── common/
    │   ├── executor/
    │   ├── lexer/
    │   ├── parser/
    │   ├── planner/
    │   ├── repl/
    │   └── storage/
    ├── tests/
    └── scripts/
```

The intent is:

- keep shared thinking in `docs/` and `examples/`
- make `java/` and `csharp/` direct mirrors of each other
- keep the engine layers explicit without adding framework-specific nesting
- leave room for tests and small helper scripts without overdesigning the repo

---

## Why implement it in both Java and C#?

Because the point is not only to build a database. The point is to understand how two serious languages express the same system.

A relational database engine naturally exercises:

- parsing and grammars
- object modeling
- trees and recursion
- generics
- collections
- iterators / enumerables
- error handling
- testing
- modular architecture
- performance tradeoffs

That makes it an unusually good comparative project.

I expect the two implementations to highlight differences in:

- AST representation
- interfaces and abstractions
- iteration models
- immutability vs mutation
- pattern matching and expression modeling
- testing ergonomics
- type systems and standard libraries

The goal is not line-by-line transliteration. The goal is to let each language reveal its character under the same architectural pressure.

---

## Educational thesis

This project is motivated by a broader question:

**What does scalability actually mean?**

A small database engine is a clean way to study that.

It will not teach manufacturing, logistics, or distributed infrastructure directly. But it will teach the deeper structure that many scaling problems share:

- bottlenecks dominate system behavior
- representation choices determine scale behavior
- logical correctness does not imply physical efficiency
- clean boundaries make systems comprehensible
- instrumentation matters more than intuition alone
- throughput and latency emerge from flow, not just from correctness

That is knowledge worth carrying into many domains.

---

## Design principles

This project will try to stay honest to a few principles:

### Keep the semantics explicit
Do not blur syntax, meaning, and execution into one layer.

### Prefer clarity over premature cleverness
A clean and inspectable engine is more valuable than a dense one.

### Build the nucleus first
The goal is to understand the essence of a relational engine before adding peripherals.

### Let performance teach, not dominate
Optimization matters, but only after the baseline design is understandable.

### Make the plan visible
An `EXPLAIN` feature is a priority because it forces architectural clarity.

### Use the second implementation to improve the design
The C# and Java versions should inform each other, not merely duplicate each other.

---

## Example target queries

```sql
CREATE TABLE users (
    id INT,
    name STRING,
    age INT
);

INSERT INTO users VALUES (1, 'Ada', 36);
INSERT INTO users VALUES (2, 'Grace', 29);

SELECT name, age
FROM users
WHERE age > 30
ORDER BY age DESC
LIMIT 10;
