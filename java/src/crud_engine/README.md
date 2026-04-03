# CRUD Engine

> AI-generated documentation. Review this document against the source before treating it as authoritative design documentation.

## Purpose

The `crud_engine` package is the storage layer above the memory allocator.
It is responsible for:

- storing object and attribute metadata
- mapping logical rows and attributes to allocator addresses
- converting between Java values and raw bytes
- managing object folders and attribute files on disk

It is not responsible for filtering, predicate evaluation, query planning, or execution strategy.
Higher layers decide which objects to read and how to interpret query semantics.
This layer only manages schema, row alignment, and typed value storage.

## Main Types

- `CrudEngineInterface`
  - public contract for schema operations, row operations, and typed reads/writes
- `CrudEngine`
  - current file-backed implementation of the CRUD storage layer
- `DatabaseSchema`
  - top-level schema model serialized to `schema.json`
- `ObjectSchema`
  - metadata for one object, including parent object name and attributes
- `AttributeSchema`
  - metadata for one attribute, including its declared type
- `TypeByteConversions`
  - helper for converting `INT`, `STRING`, `BOOL`, and `ID` values to and from bytes
- `TempBaseDemo`
  - small runnable demo that creates a sample database under `TEMP_BASE`

## Storage Model

This layer uses:

1. one JSON schema file for metadata
2. one folder per object
3. one text file per attribute
4. the memory allocator for actual value bytes

Each object row is defined by index.
If row `i` exists, then line `i` in every attribute file for that object belongs to the same logical row.

Each attribute file stores one decimal address per line:

- a non-negative number means "the value is stored at this allocator address"
- `-1` means the value is `null`

This means null values do not allocate payload bytes in the allocator.

## Files On Disk

Inside the configured database folder, `CrudEngine` manages:

- `metadata/schema.json`
  - serialized `DatabaseSchema`
- `metadata/objects/<object-slug>/`
  - one folder per object
- `metadata/objects/<object-slug>/<attribute-slug>.txt`
  - one address list per attribute
- `data/data.bin`
  - allocator-managed payload storage
- `data/bitmap.bin`
  - allocator-managed free-space bitmap

Object and attribute names are normalized to lowercase slug-style names before being used on disk.

Examples:

- `Zoo Animals` becomes `zoo_animals`
- `Animal Id` becomes `animal_id`

## Schema Model

`schema.json` stores:

- a schema version
- all known objects
- each object's canonical normalized name
- each object's optional parent object name
- each object's full attribute set
- each attribute's canonical normalized name and declared `AttributeType`

The current implementation supports single inheritance by copying the parent's attributes into the child object's schema at creation time.

Example:

1. create object `mammals`
2. add attribute `species`
3. create object `giraffes` with parent `mammals`
4. `giraffes` gets its own `species.txt` file and its own copied `species` schema entry

Parent schema changes are not automatically propagated.
If a higher layer wants descendants to stay in sync, it must explicitly call matching CRUD operations on the child objects.

## Attribute Types

The supported attribute types are:

- `INT`
  - stored as a 4-byte big-endian integer
- `STRING`
  - stored as UTF-8 bytes
- `BOOL`
  - stored as a single byte, `0` or `1`
- `ID`
  - stored as 16 raw UUID bytes

`TypeByteConversions` owns these conversions so that the public CRUD API can stay typed instead of byte-oriented.

## Object And Attribute Operations

### Create Object

When `createObject(name, parentName)` runs:

1. the object name is normalized
2. the schema is loaded
3. uniqueness is checked against normalized names
4. a new object folder is created
5. if a parent exists, the parent's attributes are copied into the child schema
6. empty attribute files are created for inherited attributes
7. the updated schema is written back to `schema.json`

### Delete Object

When `deleteObject(name)` runs:

1. the object is looked up in the schema
2. deletion is rejected if another object names it as a parent
3. every attribute file is read and all non-null allocator addresses are deleted
4. the object folder is removed
5. the schema entry is removed and saved

### Rename Object

When `renameObject(oldName, newName)` runs:

1. both names are normalized
2. uniqueness is checked
3. the object folder is renamed
4. the schema key and stored object name are updated
5. child objects that reference the old object as a parent are updated to the new normalized name
6. the schema is saved

### Create Attribute

When `createAttribute(object, attribute, type)` runs:

1. names are normalized
2. uniqueness is checked within the object
3. the current row count is derived from the object's existing attribute files
4. a new attribute file is created
5. the new file is backfilled with `-1` once per existing row
6. the schema is updated and saved

### Delete Attribute

When `deleteAttribute(object, attribute)` runs:

1. the attribute file is read
2. every non-null allocator address in the file is deleted
3. the attribute file is removed
4. the attribute is removed from the schema
5. the schema and allocator state are flushed

### Rename Attribute

When `renameAttribute(object, oldName, newName)` runs:

1. names are normalized
2. uniqueness is checked within the object
3. the attribute file is renamed
4. the schema entry key and stored attribute name are updated
5. the schema is saved

## Row Operations

### Insert Row

When `insertRow(object)` runs:

1. the object is looked up
2. insertion is rejected if the object has zero attributes
3. the current row count is derived from the existing attribute files
4. `-1` is appended to every attribute file
5. the previous row count is returned as the new row index

### Read And Write Values

Typed methods such as `writeInt(...)` or `readString(...)`:

1. normalize the names
2. load the schema
3. confirm the attribute exists and has the expected declared type
4. read the attribute address file
5. validate the row index

For writes:

- if the incoming value is `null`, the current allocation is deleted and the row entry becomes `-1`
- if the current row entry is `-1`, a new allocator entry is created
- if the current row entry already points to data, the allocator updates that value in place or relocates it

For reads:

- if the row entry is `-1`, the result is `null`
- otherwise the allocator is used to read the raw payload bytes and `TypeByteConversions` converts them to the requested Java type

### Delete Row

When `deleteRow(object, rowIndex)` runs:

1. the object is looked up
2. each attribute file is read
3. the row index is validated against each file
4. the address at that row is removed from the list
5. any non-null allocator address is deleted
6. each attribute file is rewritten without that row

This compacts all later row indexes.
So if row `3` is deleted, the old row `4` becomes the new row `3`.

## Lifecycle

Expected usage pattern:

1. construct `CrudEngine`
2. call `initialize()`
3. perform object, attribute, and row operations
4. call `close()` when done

`initialize()` ensures:

- the database root exists
- the metadata folders exist
- the allocator is initialized
- `schema.json` exists
- the schema and on-disk object/attribute files are internally consistent

`close()` closes the allocator and prevents further operations.

## Validation And Corruption Checks

The implementation currently validates:

- object names and attribute names after normalization
- uniqueness of objects and attributes by normalized name
- existence of parent objects referenced by children
- existence of object folders referenced by the schema
- existence of attribute files referenced by the schema
- row-count consistency across all attribute files in one object
- attribute type correctness during typed reads and writes
- row bounds for all row-based operations

Initialization fails if schema metadata and attribute files disagree.

## Behavior And Caveats

- Object and attribute names are stored in normalized lowercase slug form, not original display form.
- Row identity is positional, not stable.
- Deleting a row shifts later row indexes down.
- Objects with zero attributes may exist, but rows cannot be inserted into them.
- Inheritance is copied at object creation time, not dynamically linked.
- Attribute files are plain text and are fully rewritten on row delete.
- Nulls are represented by `-1`, not by a typed payload encoding.
- The layer currently optimizes for simplicity and inspectability rather than performance.

## What This Layer Does Not Do

This CRUD layer does not currently:

- execute filters or predicates
- discover descendant objects automatically for query execution
- auto-propagate parent schema changes into children
- preserve original mixed-case object or attribute display names
- maintain indexes beyond simple row-position alignment
- optimize row deletion or attribute-file rewrites

## Suggested Reading Order

If you are reading the code, this is a good order:

1. `CrudEngineInterface.java`
2. `TypeByteConversions.java`
3. `DatabaseSchema.java`
4. `ObjectSchema.java`
5. `AttributeSchema.java`
6. `CrudEngine.java`
7. `TempBaseDemo.java`
