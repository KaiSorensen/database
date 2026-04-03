package memory_allocator_tests;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import memory_allocator.BitmapMemoryAllocator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class BitmapMemoryAllocatorTest {
    private static final long BLOCK_SIZE = 4L;
    private static final String DATA_FILE = "data.bin";
    private static final String BITMAP_FILE = "bitmap.bin";

    @TempDir
    Path tempDir;

    @Test
    void initializeCreatesDatabaseFiles() throws Exception {
        Path db = tempDir.resolve("db");

        try (BitmapMemoryAllocator allocator = newAllocator(db)) {
            assertTrue(Files.isDirectory(db));
            assertTrue(Files.exists(db.resolve(DATA_FILE)));
            assertTrue(Files.exists(db.resolve(BITMAP_FILE)));
        }
    }

    @Test
    void operationsRequireInitialization() throws Exception {
        BitmapMemoryAllocator allocator = new BitmapMemoryAllocator(tempDir.resolve("db"));

        assertThrows(IllegalStateException.class, () -> allocator.create(new byte[] {1}));
        assertThrows(IllegalStateException.class, () -> allocator.read(0L));
        assertThrows(IllegalStateException.class, () -> allocator.update(0L, new byte[] {1}));
        assertThrows(IllegalStateException.class, () -> allocator.delete(0L));
        assertThrows(IllegalStateException.class, () -> allocator.isAllocated(0L));
        assertThrows(IllegalStateException.class, () -> allocator.getLength(0L));
        assertThrows(IllegalStateException.class, allocator::flush);
        assertDoesNotThrow(allocator::close);
    }

    @Test
    void createWritesLengthPayloadAndFileSizes() throws Exception {
        Path db = tempDir.resolve("db");
        byte[] payload = new byte[] {10, 20, 30};

        try (BitmapMemoryAllocator allocator = newAllocator(db)) {
            long address = allocator.create(payload);

            assertEquals(0L, address);
            try (RandomAccessFile file = openDataFile(db)) {
                assertEquals(payload.length, file.readInt());
                byte[] actual = new byte[payload.length];
                file.readFully(actual);
                assertArrayEquals(payload, actual);
            }
            assertEquals(blocksFor(payload.length) * BLOCK_SIZE, Files.size(db.resolve(DATA_FILE)));
            assertEquals(bitmapBytesForBlocks(blocksFor(payload.length)), Files.size(db.resolve(BITMAP_FILE)));
        }
    }

    @Test
    void createAndUpdateRejectNullPayloads() throws Exception {
        Path db = tempDir.resolve("db");

        try (BitmapMemoryAllocator allocator = newAllocator(db)) {
            long address = allocator.create(new byte[] {1, 2, 3});

            assertThrows(IllegalArgumentException.class, () -> allocator.create(null));
            assertThrows(IllegalArgumentException.class, () -> allocator.update(address, null));
        }
    }

    @Test
    void readReturnsOriginalPayloadIncludingZeroBytes() throws Exception {
        Path db = tempDir.resolve("db");
        byte[] payload = new byte[] {1, 0, 2, 0, 3};

        try (BitmapMemoryAllocator allocator = newAllocator(db)) {
            long address = seedAllocation(allocator, payload);
            assertArrayEquals(payload, allocator.read(address));
        }
    }

    @Test
    void getLengthReturnsStoredPayloadLength() throws Exception {
        Path db = tempDir.resolve("db");
        byte[] payload = new byte[] {7, 8, 9, 10, 11};

        try (BitmapMemoryAllocator allocator = newAllocator(db)) {
            long address = seedAllocation(allocator, payload);
            assertEquals(payload.length, allocator.getLength(address));
        }
    }

    @Test
    void isAllocatedOnlyReturnsTrueForAllocationStartAddresses() throws Exception {
        Path db = tempDir.resolve("db");
        byte[] payload = new byte[] {1, 2, 3, 4, 5};

        try (BitmapMemoryAllocator allocator = newAllocator(db)) {
            long address = seedAllocation(allocator, payload);
            assertTrue(allocator.isAllocated(address));
            assertFalse(allocator.isAllocated(address + BLOCK_SIZE));
            assertFalse(allocator.isAllocated(99L));
        }
    }

    @Test
    void isAllocatedReturnsFalseForNegativeMisalignedAndEndOfFileAddresses() throws Exception {
        Path db = tempDir.resolve("db");

        try (BitmapMemoryAllocator allocator = newAllocator(db)) {
            long address = allocator.create(new byte[] {1, 2, 3, 4});

            assertFalse(allocator.isAllocated(-BLOCK_SIZE));
            assertFalse(allocator.isAllocated(address + 1));
            assertFalse(allocator.isAllocated(Files.size(db.resolve(DATA_FILE))));
        }
    }

    @Test
    void updateWithSmallerPayloadKeepsSameAddress() throws Exception {
        Path db = tempDir.resolve("db");

        try (BitmapMemoryAllocator allocator = newAllocator(db)) {
            long address = seedAllocation(allocator, new byte[] {1, 2, 3, 4, 5, 6});
            long updated = allocator.update(address, new byte[] {9, 8});

            assertEquals(address, updated);
            assertArrayEquals(new byte[] {9, 8}, allocator.read(updated));
            assertEquals(2, allocator.getLength(updated));
        }
    }

    @Test
    void updateToSmallerPayloadFreesTrailingBlocksForReuse() throws Exception {
        Path db = tempDir.resolve("db");
        byte[] original = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9};
        byte[] smaller = new byte[] {9};
        byte[] inserted = new byte[] {7, 7, 7, 7};

        try (BitmapMemoryAllocator allocator = newAllocator(db)) {
            long address = allocator.create(original);
            long updated = allocator.update(address, smaller);
            long reused = allocator.create(inserted);

            assertEquals(address, updated);
            assertEquals(address + 8L, reused);
            assertArrayEquals(smaller, allocator.read(updated));
            assertArrayEquals(inserted, allocator.read(reused));
        }
    }

    @Test
    void updateToZeroLengthPayloadKeepsHeaderBlockAndFreesRemainingBlocks() throws Exception {
        Path db = tempDir.resolve("db");
        byte[] original = new byte[] {1, 2, 3, 4, 5};
        byte[] inserted = new byte[] {8, 8, 8, 8};

        try (BitmapMemoryAllocator allocator = newAllocator(db)) {
            long address = allocator.create(original);
            long updated = allocator.update(address, new byte[0]);
            long reused = allocator.create(inserted);

            assertEquals(address, updated);
            assertEquals(0, allocator.getLength(updated));
            assertArrayEquals(new byte[0], allocator.read(updated));
            assertEquals(address + BLOCK_SIZE, reused);
            assertArrayEquals(inserted, allocator.read(reused));
        }
    }

    @Test
    void updateWithLargerPayloadReturnsReadableAllocationAndClearsOldStart() throws Exception {
        Path db = tempDir.resolve("db");
        byte[] replacement = new byte[] {9, 8, 7, 6, 5, 4, 3, 2};

        try (BitmapMemoryAllocator allocator = newAllocator(db)) {
            long address = seedAllocation(allocator, new byte[] {1, 2});
            long updated = allocator.update(address, replacement);

            assertArrayEquals(replacement, allocator.read(updated));
            if (updated == address) {
                assertTrue(allocator.isAllocated(updated));
            } else {
                assertFalse(allocator.isAllocated(address));
                assertTrue(allocator.isAllocated(updated));
            }
        }
    }

    @Test
    void updateWithLargerPayloadAppendsWhenFreedGapIsTooSmall() throws Exception {
        Path db = tempDir.resolve("db");
        byte[] left = new byte[] {1, 1, 1, 1, 1};
        byte[] middle = new byte[] {2, 2, 2, 2};
        byte[] right = new byte[] {3, 3, 3, 3};
        byte[] larger = new byte[] {9, 9, 9, 9, 9, 9, 9, 9, 9};

        try (BitmapMemoryAllocator allocator = newAllocator(db)) {
            long leftAddress = allocator.create(left);
            long middleAddress = allocator.create(middle);
            long rightAddress = allocator.create(right);
            allocator.delete(leftAddress);

            long updatedAddress = allocator.update(middleAddress, larger);

            assertEquals(28L, updatedAddress);
            assertFalse(allocator.isAllocated(middleAddress));
            assertArrayEquals(larger, allocator.read(updatedAddress));
            assertArrayEquals(right, allocator.read(rightAddress));
        }
    }

    @Test
    void deleteClearsAllocationAndAllowsReuse() throws Exception {
        Path db = tempDir.resolve("db");

        try (BitmapMemoryAllocator allocator = newAllocator(db)) {
            long address = seedAllocation(allocator, new byte[] {1, 2, 3});
            allocator.delete(address);

            assertFalse(allocator.isAllocated(address));
            assertEquals(address, allocator.create(new byte[] {9, 9, 9}));
        }
    }

    @Test
    void negativeAddressesThrowForReadUpdateDeleteAndGetLength() throws Exception {
        Path db = tempDir.resolve("db");

        try (BitmapMemoryAllocator allocator = newAllocator(db)) {
            assertThrows(IOException.class, () -> allocator.read(-BLOCK_SIZE));
            assertThrows(IOException.class, () -> allocator.update(-BLOCK_SIZE, new byte[] {1}));
            assertThrows(IOException.class, () -> allocator.delete(-BLOCK_SIZE));
            assertThrows(IOException.class, () -> allocator.getLength(-BLOCK_SIZE));
        }
    }

    @Test
    void operationsOnDeletedAllocationThrowAndReportUnallocated() throws Exception {
        Path db = tempDir.resolve("db");

        try (BitmapMemoryAllocator allocator = newAllocator(db)) {
            long address = allocator.create(new byte[] {1, 2, 3, 4});
            allocator.delete(address);

            assertFalse(allocator.isAllocated(address));
            assertThrows(IOException.class, () -> allocator.read(address));
            assertThrows(IOException.class, () -> allocator.getLength(address));
            assertThrows(IOException.class, () -> allocator.delete(address));
            assertThrows(IOException.class, () -> allocator.update(address, new byte[] {9}));
        }
    }

    @Test
    void initializeRejectsBitmapFileWhoseLengthIsNotAWholeWord() throws Exception {
        Path db = tempDir.resolve("db");
        Files.createDirectories(db);
        Files.write(db.resolve(DATA_FILE), new byte[0]);
        Files.write(db.resolve(BITMAP_FILE), new byte[] {1});

        BitmapMemoryAllocator allocator = new BitmapMemoryAllocator(db);

        assertThrows(IOException.class, allocator::initialize);
        assertDoesNotThrow(allocator::close);
    }

    @Test
    void flushPersistsDataAndBitmapForReopen() throws Exception {
        Path db = tempDir.resolve("db");
        byte[] payload = new byte[] {4, 0, 5, 0, 6};
        long address;

        try (BitmapMemoryAllocator allocator = newAllocator(db)) {
            address = seedAllocation(allocator, payload);
            allocator.flush();
            assertEquals(bitmapBytesForBlocks(blocksFor(payload.length)), Files.size(db.resolve(BITMAP_FILE)));
            try (RandomAccessFile file = openDataFile(db)) {
                assertEquals(payload.length, file.readInt());
            }
        }

        try (BitmapMemoryAllocator reopened = newAllocator(db)) {
            assertArrayEquals(payload, reopened.read(address));
        }
    }

    @Test
    void flushPersistsAllocationsAcrossBitmapWordBoundary() throws Exception {
        Path db = tempDir.resolve("db");
        long lastAddress = -1L;
        byte[] lastPayload = new byte[] {32};

        try (BitmapMemoryAllocator allocator = newAllocator(db)) {
            for (int i = 0; i < 33; i++) {
                byte[] payload = new byte[] {(byte) i};
                lastAddress = allocator.create(payload);
                lastPayload = payload;
            }

            allocator.flush();

            assertEquals(32L * 8L, lastAddress);
            assertEquals(16L, Files.size(db.resolve(BITMAP_FILE)));
        }

        try (BitmapMemoryAllocator reopened = newAllocator(db)) {
            assertTrue(reopened.isAllocated(lastAddress));
            assertArrayEquals(lastPayload, reopened.read(lastAddress));
        }
    }

    @Test
    void closePersistsStateForReopen() throws Exception {
        Path db = tempDir.resolve("db");
        byte[] payload = new byte[] {3, 1, 4, 1, 5};
        long address;

        try (BitmapMemoryAllocator allocator = newAllocator(db)) {
            address = seedAllocation(allocator, payload);
            assertDoesNotThrow(allocator::close);
        }

        try (BitmapMemoryAllocator reopened = newAllocator(db)) {
            assertArrayEquals(payload, reopened.read(address));
        }
    }

    @Test
    void zeroLengthPayloadCanBeCreatedReadAndDeleted() throws Exception {
        Path db = tempDir.resolve("db");

        try (BitmapMemoryAllocator allocator = newAllocator(db)) {
            long address = allocator.create(new byte[0]);

            assertEquals(0L, address);
            assertEquals(0, allocator.getLength(address));
            assertArrayEquals(new byte[0], allocator.read(address));
            allocator.delete(address);
            assertFalse(allocator.isAllocated(address));
        }
    }

    @Test
    void exactBlockBoundaryPayloadUsesExpectedNumberOfBlocks() throws Exception {
        Path db = tempDir.resolve("db");

        try (BitmapMemoryAllocator allocator = newAllocator(db)) {
            long first = allocator.create(new byte[] {1, 2, 3, 4});
            long second = allocator.create(new byte[] {9});

            assertEquals(0L, first);
            assertEquals(8L, second);
        }
    }

    @Test
    void oneBytePastBlockBoundaryUsesNextWholeBlock() throws Exception {
        Path db = tempDir.resolve("db");

        try (BitmapMemoryAllocator allocator = newAllocator(db)) {
            long first = allocator.create(new byte[] {1, 2, 3, 4, 5});
            long second = allocator.create(new byte[] {9});

            assertEquals(0L, first);
            assertEquals(12L, second);
        }
    }

    @Test
    void adjacentAllocationsRemainReadableAcrossMultipleCreates() throws Exception {
        Path db = tempDir.resolve("db");
        byte[] a = new byte[] {1, 2, 3, 4};
        byte[] b = new byte[] {5, 6, 7, 8};
        byte[] c = new byte[] {9, 10, 11, 12};

        try (BitmapMemoryAllocator allocator = newAllocator(db)) {
            long addressA = allocator.create(a);
            long addressB = allocator.create(b);
            long addressC = allocator.create(c);

            assertEquals(0L, addressA);
            assertEquals(8L, addressB);
            assertEquals(16L, addressC);
            assertArrayEquals(a, allocator.read(addressA));
            assertArrayEquals(b, allocator.read(addressB));
            assertArrayEquals(c, allocator.read(addressC));
        }
    }

    @Test
    void deletingSecondAdjacentAllocationShouldNotDependOnPreviousBlockBeingClear() throws Exception {
        Path db = tempDir.resolve("db");

        try (BitmapMemoryAllocator allocator = newAllocator(db)) {
            allocator.create(new byte[] {1, 2, 3, 4});
            long second = allocator.create(new byte[] {5, 6, 7, 8});

            assertDoesNotThrow(() -> allocator.delete(second));
            assertFalse(allocator.isAllocated(second));
        }
    }

    @Test
    void updatingSecondAdjacentAllocationShouldWorkIndependently() throws Exception {
        Path db = tempDir.resolve("db");

        try (BitmapMemoryAllocator allocator = newAllocator(db)) {
            allocator.create(new byte[] {1, 2, 3, 4});
            long second = allocator.create(new byte[] {5, 6, 7, 8});
            long updated = allocator.update(second, new byte[] {9, 9});

            assertEquals(second, updated);
            assertArrayEquals(new byte[] {9, 9}, allocator.read(updated));
        }
    }

    @Test
    void deletingMiddleAllocationPreservesNeighborsAndReusesGap() throws Exception {
        Path db = tempDir.resolve("db");
        byte[] left = new byte[] {1, 1, 1, 1};
        byte[] middle = new byte[] {2, 2, 2, 2};
        byte[] right = new byte[] {3, 3, 3, 3};

        try (BitmapMemoryAllocator allocator = newAllocator(db)) {
            long leftAddress = allocator.create(left);
            long middleAddress = allocator.create(middle);
            long rightAddress = allocator.create(right);

            allocator.delete(middleAddress);
            long reused = allocator.create(new byte[] {8, 8, 8, 8});

            assertArrayEquals(left, allocator.read(leftAddress));
            assertArrayEquals(right, allocator.read(rightAddress));
            assertEquals(middleAddress, reused);
        }
    }

    @Test
    void updatingFirstAllocationLargerDoesNotCorruptAdjacentAllocation() throws Exception {
        Path db = tempDir.resolve("db");
        byte[] right = new byte[] {7, 7, 7, 7};
        byte[] larger = new byte[] {1, 2, 3, 4, 5, 6, 7, 8};

        try (BitmapMemoryAllocator allocator = newAllocator(db)) {
            long leftAddress = allocator.create(new byte[] {1, 1});
            long rightAddress = allocator.create(right);
            long updatedAddress = allocator.update(leftAddress, larger);

            assertArrayEquals(larger, allocator.read(updatedAddress));
            assertArrayEquals(right, allocator.read(rightAddress));
        }
    }

    @Test
    void invalidAddressesThrowForReadDeleteAndGetLength() throws Exception {
        Path db = tempDir.resolve("db");

        try (BitmapMemoryAllocator allocator = newAllocator(db)) {
            long address = allocator.create(new byte[] {1, 2, 3});

            assertThrows(IOException.class, () -> allocator.read(address + 1));
            assertThrows(IOException.class, () -> allocator.delete(address + BLOCK_SIZE));
            assertThrows(IOException.class, () -> allocator.getLength(400L));
        }
    }

    @Test
    void flushAndReopenAfterDeletePreservesFreedState() throws Exception {
        Path db = tempDir.resolve("db");
        long first;
        long second;

        try (BitmapMemoryAllocator allocator = newAllocator(db)) {
            first = allocator.create(new byte[] {1, 1, 1, 1});
            second = allocator.create(new byte[] {2, 2, 2, 2});
            allocator.delete(first);
            allocator.flush();
        }

        try (BitmapMemoryAllocator reopened = newAllocator(db)) {
            assertFalse(reopened.isAllocated(first));
            assertTrue(reopened.isAllocated(second));
            assertEquals(first, reopened.create(new byte[] {9, 9, 9, 9}));
        }
    }

    @Test
    void sequenceOfChangesKeepsAllocatorStateConsistent() throws Exception {
        Path db = tempDir.resolve("db");
        byte[] a = new byte[] {1, 1, 1, 1};
        byte[] b = new byte[] {2, 2, 2, 2, 2, 2, 2, 2};
        byte[] bSmaller = new byte[] {7, 7, 7, 7};
        byte[] c = new byte[] {3, 3, 3, 3};
        byte[] d = new byte[] {4, 4, 4, 4, 4, 4, 4, 4};

        try (BitmapMemoryAllocator allocator = newAllocator(db)) {
            long addressA = allocator.create(a);
            long addressB = allocator.create(b);
            allocator.delete(addressA);

            long updatedB = allocator.update(addressB, bSmaller);
            long addressC = allocator.create(c);
            allocator.delete(updatedB);
            long addressD = allocator.create(d);

            assertEquals(0L, addressA);
            assertEquals(8L, addressB);
            assertEquals(addressB, updatedB);
            assertEquals(addressA, addressC);
            assertEquals(addressB, addressD);

            assertArrayEquals(c, allocator.read(addressC));
            assertArrayEquals(d, allocator.read(addressD));
            assertFalse(allocator.isAllocated(20L));
            assertTrue(allocator.isAllocated(addressC));
            assertTrue(allocator.isAllocated(addressD));
        }

        try (BitmapMemoryAllocator reopened = newAllocator(db)) {
            assertArrayEquals(c, reopened.read(0L));
            assertArrayEquals(d, reopened.read(8L));
            assertTrue(reopened.isAllocated(0L));
            assertTrue(reopened.isAllocated(8L));
            assertFalse(reopened.isAllocated(20L));
        }
    }

    private BitmapMemoryAllocator newAllocator(Path db) throws IOException {
        BitmapMemoryAllocator allocator = new BitmapMemoryAllocator(db);
        allocator.initialize();
        return allocator;
    }

    private RandomAccessFile openDataFile(Path db) throws IOException {
        return new RandomAccessFile(db.resolve(DATA_FILE).toFile(), "r");
    }

    private long seedAllocation(BitmapMemoryAllocator allocator, byte[] payload) throws IOException {
        return allocator.create(payload);
    }

    private long blocksFor(int payloadLength) {
        long totalBytes = BLOCK_SIZE + payloadLength;
        return (totalBytes + BLOCK_SIZE - 1) / BLOCK_SIZE;
    }

    private long bitmapBytesForBlocks(long blockCount) {
        return ((blockCount + Long.SIZE - 1) / Long.SIZE) * Long.BYTES;
    }
}
