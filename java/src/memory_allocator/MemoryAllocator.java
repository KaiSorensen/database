package memory_allocator;

import java.io.IOException;

/**
 * Contract for the lowest storage layer in the database.
 *
 * <p>This interface owns raw byte storage and free-space tracking for the main database file.
 * It does not know about tables, columns, schemas, or query semantics. Callers in higher layers
 * are responsible for tracking logical metadata such as which address belongs to which field and
 * how many bytes should be read for a value.</p>
 */
public interface MemoryAllocator extends AutoCloseable {

    /**
     * Opens any backing files and prepares allocator metadata for use.
     */
    void initialize() throws IOException;

    /**
     * Allocates space for the provided payload, writes it into the database file,
     * and returns the starting address of the stored bytes.
     */
    long create(byte[] data) throws IOException;

    /**
     * Reads {@code length} bytes starting at {@code address}.
     */
    byte[] read(long address, int length) throws IOException;

    /**
     * Replaces the value stored at {@code address}.
     *
     * <p>The implementation may reuse the existing allocation or relocate the value if the new
     * payload requires a different amount of space. The returned address is therefore the current
     * address of the updated value and may differ from the original address.</p>
     */
    long update(long address, int currentLength, byte[] newData) throws IOException;

    /**
     * Deletes the allocation that begins at {@code address} and spans {@code length} bytes.
     */
    void delete(long address, int length) throws IOException;

    /**
     * Returns whether the entire range is currently marked as allocated.
     */
    boolean isAllocated(long address, int length) throws IOException;

    /**
     * Flushes any buffered file or bitmap state to disk.
     */
    void flush() throws IOException;

    /**
     * Flushes and releases any system resources held by the allocator.
     */
    @Override
    void close() throws IOException;
}
