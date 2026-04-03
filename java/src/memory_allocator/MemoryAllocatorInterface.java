package memory_allocator;

import java.io.IOException;

/**
 * Contract for the lowest storage layer in the database.
 *
 * <p>This interface owns raw byte storage and free-space tracking for the main database file.
 * It does not know about tables, columns, schemas, or query semantics. Callers in higher layers
 * are responsible for tracking logical metadata such as which address belongs to which field.</p>
 *
 * <p>Each allocation stores the payload length as a 4-byte integer at the beginning of the
 * allocated space, followed by the payload bytes. Higher layers therefore only need to retain the
 * starting address returned by {@link #create(byte[])}.</p>
 */
public interface MemoryAllocatorInterface extends AutoCloseable {

    /**
     * Opens any backing files and prepares allocator metadata for use.
     */
    void initialize() throws IOException;

    /**
     * Allocates space for the provided payload, writes it into the database file,
     * stores the payload length in the first 4 bytes, and returns the starting
     * address of the allocation.
     */
    long create(byte[] data) throws IOException;

    /**
     * Reads the payload stored at {@code address}.
     *
     * <p>The allocator reads the 4-byte length header and then returns exactly that
     * many bytes from the stored payload.</p>
     */
    byte[] read(long address) throws IOException;

    /**
     * Replaces the value stored at {@code address}.
     *
     * <p>The implementation may reuse the existing allocation or relocate the value if the new
     * payload requires a different amount of space. The returned address is therefore the current
     * address of the updated value and may differ from the original address.</p>
     */
    long update(long address, byte[] newData) throws IOException;

    /**
     * Deletes the allocation stored at {@code address}.
     *
     * <p>The allocator reads the 4-byte length header to determine how much space to free.</p>
     */
    void delete(long address) throws IOException;

    /**
     * Returns whether an allocation begins at {@code address}.
     */
    boolean isAllocated(long address) throws IOException;

    /**
     * Returns the payload length stored in the 4-byte header for the allocation at {@code address}.
     */
    int getLength(long address) throws IOException;

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
