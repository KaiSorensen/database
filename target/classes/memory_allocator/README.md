# Memory Allocator

> AI-generated documentation. Review this document against the source before treating it as authoritative design documentation.

## Purpose

The `memory_allocator` package is the lowest storage layer in the database.
It is responsible for:

- storing raw payload bytes
- tracking which regions of the data file are in use
- returning stable byte addresses that higher layers can store as references

It is not responsible for schemas, tables, columns, rows, or query logic.
Higher layers decide what a stored value means. The allocator only knows how to place bytes in storage and find them again.

## Main Types

- `MemoryAllocator`
  - interface for allocator lifecycle and CRUD-like storage operations
- `BitmapMemoryAllocator`
  - current implementation backed by a data file plus a bitmap file
- `LongBitMap`
  - sparse in-memory bitmap used to track allocated blocks
- `LengthBlock`
  - converts between the fixed-size length header block and a numeric payload length

## Storage Model

The allocator uses a fixed block size of `4` bytes.

Every stored value is laid out like this:

1. one 4-byte header block containing the payload length
2. the payload bytes
3. optional unused padding at the end of the last block

So a payload of length `N` consumes:

`4 + N` total bytes, rounded up to the next multiple of `4`

Examples:

- payload length `0` uses `1` block
- payload length `1` uses `2` blocks
- payload length `4` uses `2` blocks
- payload length `5` uses `3` blocks

The address returned by `create(...)` is the byte offset of the first header block, not the first payload byte.

## Files On Disk

Inside the configured database folder, `BitmapMemoryAllocator` manages:

- `data.bin`
  - raw allocation contents
- `bitmap.bin`
  - serialized allocation bitmap

The bitmap tracks allocation at block granularity, not byte granularity.
If block `k` is allocated, then bytes `[k * 4, (k + 1) * 4)` belong to some allocation.

## Allocation Flow

When `create(byte[] data)` runs:

1. it checks initialization and validates the input
2. it computes how many 4-byte blocks are needed
3. it searches the bitmap for the next clear run of that many blocks
4. if the data file is too short, it extends both files
5. it writes the 4-byte length header and then the payload bytes into `data.bin`
6. it marks the relevant bitmap range as allocated
7. it returns the starting byte address

Free-space reuse is first-fit based on the current bitmap scan.

## Read Flow

When `read(address)` runs:

1. it verifies the allocator is initialized
2. it verifies that `address` is the start of an allocated region
3. it reads the first 4 bytes as the stored payload length
4. it reads exactly that many bytes after the header

If the address is invalid, misaligned, deleted, or outside the file, the allocator throws `IOException`.

## Update Flow

`update(address, newData)` has two modes:

- In-place update
  - used when the new payload fits within the same number of blocks
  - the header is rewritten
  - the new payload bytes are written over the old bytes
  - if the new value uses fewer blocks, the trailing blocks are cleared in the bitmap

- Relocating update
  - used when the new payload needs more blocks than the old allocation owns
  - the allocator creates a new allocation elsewhere
  - the old allocation is deleted
  - the new address is returned

Because of relocation, callers must always use the address returned by `update(...)`.

## Delete Flow

`delete(address)`:

1. validates that the address points to the start of an allocation
2. reads the stored payload length
3. computes how many blocks that allocation owns
4. clears those blocks in the bitmap

Deletion does not compact the file. Space becomes reusable, but `data.bin` does not shrink.

## Bitmap Details

`LongBitMap` stores bits sparsely as 64-bit words in a `TreeMap`.

Important properties:

- missing words are treated as all-zero
- allocated ranges are tracked in memory during operation
- `flush()` writes bitmap words to `bitmap.bin`
- `initialize()` reconstructs the bitmap by reading those words back

The serialized bitmap file length must be a multiple of `8` bytes because each persisted word is a Java `long`.

## Length Header Details

The first block of each allocation stores the payload length in big-endian form.

Current practical constraints:

- header block size is `4` bytes
- payload length must fit within Java array limits
- the implementation explicitly rejects lengths above `Integer.MAX_VALUE`

## Lifecycle

Expected usage pattern:

1. construct `BitmapMemoryAllocator`
2. call `initialize()`
3. perform `create`, `read`, `update`, `delete`, `isAllocated`, `getLength`
4. call `flush()` when persistence is needed
5. call `close()` when done

`close()` attempts to flush only if initialization completed far enough for flushing to be valid, then closes open file handles.

## Behavior And Caveats

- Addresses must be block-aligned.
- An address is valid only if it points to the start of an allocation.
- Interior addresses inside a live allocation are not valid read/update/delete targets.
- `isAllocated(address)` is stricter than "the block is marked used"; it checks that the address appears to be the true start of a coherent allocation.
- Updating to a larger value may move the allocation.
- Updating to a smaller value can free trailing blocks for reuse.
- Zero-length payloads are valid and still occupy one header block.
- The allocator currently reuses freed space, but does not compact or defragment the data file.
- File size can therefore grow over time even if many allocations are later deleted.

## What This Layer Does Not Do

This allocator does not currently:

- store logical metadata about ownership of addresses
- validate higher-level schema semantics
- compact the file after deletes
- guarantee optimal packing beyond first-fit reuse
- support payload sizes beyond Java array constraints

## Suggested Reading Order

If you are reading the code, this is a good order:

1. `MemoryAllocator.java`
2. `BitmapMemoryAllocator.java`
3. `LongBitMap.java`
4. `LengthBlock.java`
