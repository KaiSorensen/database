# Database Implementation Architecture

This project is a small database system with a clean layered design.

The main idea is:

- the **language layer** understands queries
- the **executor layer** decides what to do
- the **storage layers** know how to find and store data
- the **allocator** manages raw space in one big memory file

---

## High-level picture

A query flows through the system like this:

1. User writes a query in the custom database language
2. The parser turns that query into a tree
3. The executor walks that tree and decides what operations are needed
4. The executor asks the storage system for data
5. The storage system reads addresses from column files
6. Those addresses point into one large memory file
7. The allocator reads or writes the actual bytes in that memory file

So the system separates:

- **meaning**
- **database operations**
- **physical storage**
- **raw memory management**

---

# Main layers

## 1. Language layer

This layer handles the query language.

Its job is to:

- read query text
- split it into tokens
- parse the tokens
- build an AST (abstract syntax tree)

This layer does **not** execute the query.
It only understands the structure of the query.

Example responsibilities:

- lexer
- parser
- AST node classes
- syntax errors

---

## 2. Query executor layer

This layer takes the AST and performs the requested work.

Its job is to:

- interpret the query tree
- decide what data must be fetched
- apply filters and functions
- combine results
- return final output

This is the most complex layer because it sits between the language and the storage system.

Example responsibilities:

- `SELECT`
- `INSERT`
- `DELETE`
- filtering
- function execution
- projection
- maybe joins later
- maybe indexing later

This layer should **not** manage raw bytes directly.
It should ask lower layers for data.

---

## 3. Table / column access layer

This layer understands the database’s logical storage layout.

Its job is to:

- know what tables exist
- know what columns exist
- know how rows are represented across column files
- read and write addresses for column values
- serialize and deserialize typed values when needed

This layer is the bridge between:
- logical database concepts
- physical storage concepts

Example responsibilities:

- fetch values from a column
- insert a value into a column
- delete a row’s value from a column
- map row positions to addresses
- understand column types

This layer knows about:
- tables
- columns
- row IDs or row positions
- types
- addresses

But it should **not** know query grammar.

---

## 4. Memory allocator layer

This is the lowest storage layer.

Its job is to manage one large memory file that holds actual value bytes.

It should do things like:

- allocate a block of size `n`
- free a block
- reuse free space
- append to the end if needed
- track used/free space with metadata
- read bytes from an address
- write bytes to an address

This layer should know **nothing** about:
- tables
- columns
- strings vs integers in a semantic sense
- query execution

It only manages space and bytes.

You are considering implementing this with something like a:
- bitmap
- buddy allocator
- free block system

---

# Physical storage design

## One metadata file

There will be a metadata file that stores schema information.

This may include:

- table names
- column names
- column types
- maybe table IDs / column IDs
- maybe other schema metadata later

This file describes the structure of the database.

---

## One folder per table

Each table gets its own folder.

That folder contains the files related to that table.

This keeps the database organized by table.

---

## One file per column

Each column in a table has its own file.

This means the database is physically **column-oriented**.

Instead of storing a whole row in one place, the system stores each column separately.

Benefits:

- adding a new column is easy
- dropping a column is easy
- schema evolution is easier
- columns are modular

Tradeoff:

- reading a whole row is more expensive because values must be gathered from multiple column files

---

## Column files store addresses, not full payloads

The column files mostly store metadata and references.

For each row position, the column file stores something like:

- the address of the actual value in the memory file
- maybe the size of the value
- maybe null / deleted markers
- maybe row status metadata

So the column file acts like a map from:

`(table, column, row)` -> `memory address`

---

## One large memory file

Actual payload data lives in one big memory file.

This is where real values are stored, such as:

- strings
- numbers
- serialized values
- maybe large text
- maybe embeddings later

The column files do not hold the full values inline.
They only point to where those values live.

This makes variable-length data much easier to handle.

For example:

- a short string and a long string can both be stored
- rows do not overflow fixed page sizes
- large values do not force table files to be reorganized

---

# Why this design was chosen

This design is optimized mainly for:

- modularity
- schema flexibility
- scalability of structure
- easier handling of variable-length data

The core idea is to accept more indirection in exchange for cleaner growth.

In other words:

- reads may involve more address lookups
- full-row retrieval may touch multiple files
- but adding columns and handling flexible data becomes much easier

So the design favors:

- **structural scalability**
- **clean separation of responsibility**

over:

- **minimal read overhead**

---

# Main tradeoffs

## Benefits

- clean separation of layers
- easy to reason about each subsystem
- columns can be added without rewriting an entire row-based table file
- variable-length values are easy to support
- allocator is reusable and independent
- architecture is modular

## Costs

- more pointer chasing / address indirection
- reading a full row is slower
- more file coordination
- more metadata management
- possible fragmentation in the memory file
- executor and storage coordination become more complex

---

# Simple mental model

You can think of the system like this:

- **language layer** = understands the sentence
- **executor layer** = decides what actions to perform
- **column layer** = knows where table values are logically referenced
- **allocator layer** = knows where raw bytes physically live

Or even shorter:

- parser = understands
- executor = orchestrates
- column store = maps
- allocator = stores

---

# Example flow

For a query like:

`SELECT name FROM users WHERE age > 30`

the flow would be:

1. Lexer produces tokens
2. Parser builds an AST
3. Executor sees:
   - table = `users`
   - filter = `age > 30`
   - projection = `name`
4. Executor asks storage layer for `age` column values
5. Storage layer reads row-address references from the `age` column file
6. Those addresses point into the memory file
7. Allocator reads the actual stored values
8. Executor applies `age > 30`
9. Executor asks for `name` values for matching rows
10. Storage layer reads those addresses
11. Allocator retrieves the actual name bytes
12. Executor returns the result set

---

# Current architectural summary

This database is a **layered, column-oriented system** where:

- queries are parsed into a tree
- an executor interprets the tree
- tables are stored as folders of column files
- column files store references to values
- actual values live in one allocator-managed memory file

---

# Good design rule for implementation

Each layer should know only what it must know.

- The parser should not know file layout.
- The executor should not know raw byte allocation details.
- The allocator should not know what a table is.
- The column/storage layer should connect logical database structure to physical storage.

That separation is one of the main strengths of this architecture.