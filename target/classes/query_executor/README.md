# Query Execution Architecture

Educational SQL engine notes.

## 1. Core Insight

The parse tree is not just a representation of the query. It is the execution engine.

Instead of translating the parse tree into a separate execution plan or instruction list, each node in the tree becomes an executable unit. Execution emerges naturally by recursively evaluating the tree.

## 2. High-Level Model

The system has two primary components:

- Node hierarchy: execution and structure
- Central data structure: `ColumnsAndRows`

```text
        Query (SQL)
             ↓
        Parse Tree (Nodes)
             ↓
   Recursive Execution (nodes execute themselves)
             ↓
     Shared Data Structure (ColumnsAndRows)
```

## 3. The Data Structure: `ColumnsAndRows`

This is the state container for all computation.

### Responsibilities

- Store rows and columns
- Support operations such as:
  - `filter(...)`
  - `select(...)`
  - `addColumn(...)`
  - `aggregate(...)`
- Hold intermediate temporary columns
- Allow referencing columns by name

### Key Property

It is mutable and accumulative. It grows as operations are applied.

```java
class ColumnsAndRows {
    Map<String, Column> columns;

    void filter(Predicate<Row> condition);
    void addColumn(String name, Function<Row, Object> computation);
    Object aggregate(String columnName, AggregationType type);
}
```

## 4. The Node Hierarchy

The real engine is the node hierarchy.

Each node represents a unit of computation and contains:

- Its type, such as `SELECT`, `FILTER`, or `AGGREGATE`
- References to child nodes
- Logic to execute itself

### Core Interface

```java
interface Node {
    void execute(ColumnsAndRows data);
}
```

## 5. Execution Model: Recursive Evaluation

Execution is tree traversal, specifically post-order:

1. Execute all child nodes.
2. Execute the current node.

```java
void execute(Node node, ColumnsAndRows data) {
    for (Node child : node.children()) {
        execute(child, data);
    }
    node.execute(data);
}
```

### Base Case

A node with no children, for example:

- Table scan
- Literal
- Simple column reference

## 6. Example: Nested Computation

### Query Concept

Create two derived columns, average each, then average the averages.

### Tree

```text
        AVG
       /   \
   AVG       AVG
   |         |
 ColA'     ColB'
```

### Execution Flow

1. Compute `ColA'` and store it in `ColumnsAndRows`.
2. Compute `ColB'` and store it in the same structure.
3. Compute `AVG(ColA')`.
4. Compute `AVG(ColB')`.
5. Compute the final average.

All intermediate values live in the same container.

## 7. Why This Works

### No Need for a Separate Coordinator

You may initially expect this:

```text
Parse Tree → Coordinator → Execution
```

But the final architecture is this:

```text
Parse Tree (nodes with behavior) = Execution Engine
```

Each node:

- Knows what to do
- Knows when to do it, after its children

## 8. Handling Parallel Dependencies

Concern:

> What if two computations must happen before a third?

Resolution:

- They are siblings in the tree.
- Both execute before the parent node.
- Their results are stored in the shared data structure.

No special coordination is needed. The tree structure already encodes dependency order.

## 9. Sequential vs. Hierarchical Tension

The core difficulty came from trying to force:

```text
A hierarchical structure (tree)
into
A linear execution model (sequence)
```

Resolution:

- Execution is sequential in time.
- Execution is defined by hierarchy.

This is a general pattern:

```text
Hierarchy defines dependencies → recursion linearizes execution
```

## 10. Design Pattern Summary

This architecture combines several key ideas:

### 1. Composite Pattern

- Nodes contain nodes.
- Tree structure represents computation.

### 2. Interpreter Pattern

- Each node interprets itself.

### 3. Shared Mutable State

- `ColumnsAndRows` acts as working memory.

### 4. Recursive Evaluation

- Recursion naturally resolves dependencies.

## 11. Alternative You Avoided

You explicitly avoided:

- Giant `if` / `else` chains
- Flat instruction lists
- External orchestration layers

Those approaches:

- Lose structure
- Become brittle
- Do not scale with complexity

## 12. Generalizable Insight

This leads to a reusable mental model:

When a system allows nested, composable operations, represent it as a tree where each node is executable.

Then:

- Let structure encode dependencies
- Let recursion handle execution order
- Use shared state, or explicit inputs and outputs, for data flow

## 13. Final Mental Model

```text
Nodes          = Logic + Structure
Tree           = Dependency Graph
Recursion      = Execution Engine
Data Structure = Shared Memory
```

No separate coordinator is required. The system is self-executing by design.
