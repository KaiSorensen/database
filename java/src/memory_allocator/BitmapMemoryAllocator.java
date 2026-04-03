package memory_allocator;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Bitmap-backed implementation skeleton for the memory allocator layer.
 *
 * <p>This class owns two files inside a database folder:
 * one for the raw data payloads and one for the bitmap that tracks allocated space.</p>
 */
public class BitmapMemoryAllocator implements MemoryAllocatorInterface {
    private static final String DATA_FILE_NAME = "data.bin";
    private static final String BITMAP_FILE_NAME = "bitmap.bin";

    // private boolean debug = false;

    // block size in number of bytes 
    // must not exceed 8 bytes (a long) because we use longs to represent the value. this is a fundamental database item size limitation. anyone who puts that much data into one of these is ridiculous anyway. but it's good to know.
    // correction: it must not exceed 4 bytes because Java arrays are indexed by integers. the best you could do is have it read a block at a time but then you would need the block size to correspond to a primitive type like long or int 
    private final long BLOCK_SIZE = 4; 
    private final Path databaseFolderPath;
    private final Path dataFilePath;
    private final Path bitmapFilePath;

    private LongBitMap bitmap;
    private RandomAccessFile dataFile;
    private RandomAccessFile bitmapFile;

    /**
     * Creates an allocator rooted at the provided database folder.
     */
    public BitmapMemoryAllocator(Path databaseFolderPath) {
        this.databaseFolderPath = databaseFolderPath;
        this.dataFilePath = databaseFolderPath.resolve(DATA_FILE_NAME);
        this.bitmapFilePath = databaseFolderPath.resolve(BITMAP_FILE_NAME);
    }

    // // constructor with debug
    // public BitmapMemoryAllocator(Path databaseFolderPath, boolean debug) {
    //     this(databaseFolderPath);
    //     this.debug = true;
    // }


    @Override
    public long create(byte[] data) throws IOException {
        // ensure that the file read/write is ready
        ensureInitialized();
        if (data == null) {
            throw new IllegalArgumentException("data array cannot be null"); // shouldn't ever happen
        }

        long blocksNeeded = getBlocksNeededForLength(data.length);

        long startBit = findNextAvailableSequence(blocksNeeded);
        // again, round up to be safe (by adding block_size - 1), but this should always be an even division until the block size is changed
        long currentFileBlockCount = (dataFile.length() + BLOCK_SIZE - (long)1)/ BLOCK_SIZE;
        long requiredBlocks = startBit + blocksNeeded;
        if (requiredBlocks > currentFileBlockCount) {
            long additionalBlocks = requiredBlocks - currentFileBlockCount;
            extendFiles(additionalBlocks);
        }
        // by now we have found/created enough space for the new data

        // get the starting byte index in the data file
        long startByte = startBit * BLOCK_SIZE;
        // move the cursor to the position of the starting byte
        dataFile.seek(startByte);
        // write the data at that location (excuse the casting)
        writeStoredLength(data.length);
        dataFile.write(data);
        // update the bitmap to reflect the presence of the new data
        bitmap.set(startBit, startBit + blocksNeeded);

        // return the byte index (address) of the stored data so that it can be stored as the object/attribute pairing
        return startByte;
    }

    @Override
    public byte[] read(long address) throws IOException {
        ensureInitialized();

        long datalength = readStoredLength(address);
        dataFile.seek(address + BLOCK_SIZE);


        /** NOTE: THE FUNDAMENTAL DATA LENGTH LIMITATION COMES FROM JAVA ARRAY INDEXING */  
        byte[] data = new byte[(int) datalength]; // another fundamental limitation: you can't have a java array bigger than an integer?
        for (int i = 0; i < datalength; i++) {
            data[i] = (byte) dataFile.readUnsignedByte();
        }

        return data;
    }

    @Override
    public long update(long address, byte[] newData) throws IOException {
        ensureInitialized();
        if (newData == null) {
            throw new IllegalArgumentException("data array cannot be null");
        }

        long startBit = requireAllocatedStartBit(address);
        long oldLength = readStoredLength(address);
        long oldBlocksUsed = getBlocksNeededForLength(oldLength);
        long newBlocksUsed = getBlocksNeededForLength(newData.length);

        if (newBlocksUsed <= oldBlocksUsed) {
            dataFile.seek(address);
            writeStoredLength(newData.length);
            dataFile.write(newData);
            if (newBlocksUsed < oldBlocksUsed) {
                bitmap.clear(startBit + newBlocksUsed, startBit + oldBlocksUsed);
            }
            return address;
        }

        long newAddress = create(newData);
        delete(address);
        return newAddress;
    }

    @Override
    public void delete(long address) throws IOException {
        ensureInitialized();

        long startBit = requireAllocatedStartBit(address);
        long dataLength = readStoredLength(address);
        long blocksUsed = getBlocksNeededForLength(dataLength);
        bitmap.clear(startBit, startBit + blocksUsed);
    }

    @Override
    public boolean isAllocated(long address) throws IOException {
        ensureInitialized();

        if (address < 0 || address % BLOCK_SIZE != 0) {
            return false;
        }

        long startBit = address / BLOCK_SIZE;
        long currentFileBlockCount = dataFile.length() / BLOCK_SIZE;
        if (startBit >= currentFileBlockCount) {
            return false;
        }

        if (!bitmap.get(startBit)) {
            return false;
        }

        if (address > dataFile.length() - BLOCK_SIZE) {
            return false;
        }

        long dataLength = readStoredLengthValueAt(address);
        long blocksUsed = getBlocksNeededForLength(dataLength);
        long endBitExclusive = startBit + blocksUsed;
        if (endBitExclusive > currentFileBlockCount) {
            return false;
        }

        for (long bit = startBit; bit < endBitExclusive; bit++) {
            if (!bitmap.get(bit)) {
                return false;
            }
        }

        return true;
    }

    @Override
    public int getLength(long address) throws IOException {
        ensureInitialized();

        long dataLength = readStoredLength(address);
        if (dataLength > Integer.MAX_VALUE) {
            throw new IOException("stored data length exceeds Java array limits");
        }
        return (int) dataLength;
    }


    ////////////////////////////
    /// MANAGEMENT FUNCTIONS ///
    ////////////////////////////
    @Override
    public void initialize() throws IOException {
        Files.createDirectories(databaseFolderPath);
        dataFile = new RandomAccessFile(dataFilePath.toFile(), "rw");
        bitmapFile = new RandomAccessFile(bitmapFilePath.toFile(), "rw");
        bitmap = loadBitmap();
    }

    @Override
    public void flush() throws IOException {
        ensureInitialized();

        // TODO: why aren't we rounding up here
        long blockCapacity = dataFile.length() / BLOCK_SIZE;
        long requiredBitmapBytes = getBitmapByteLength(blockCapacity);
        bitmapFile.setLength(requiredBitmapBytes);
        bitmapFile.seek(0);

        long wordCount = getBitmapWordCount(blockCapacity);
        for (long wordIndex = 0; wordIndex < wordCount; wordIndex++) {
            bitmapFile.writeLong(bitmap.getWordValue(wordIndex));
        }

        // TODO: I don't know what sync() does or why it's called
        bitmapFile.getFD().sync();
        dataFile.getFD().sync();
    }

    @Override
    public void close() throws IOException {
        IOException thrown = null;

        try {
            if (bitmapFile != null && dataFile != null && bitmap != null) {
                flush();
            }
        } catch (IOException exception) {
            thrown = exception;
        }

        try {
            if (bitmapFile != null) {
                bitmapFile.close();
            }
        } catch (IOException exception) {
            if (thrown == null) {
                thrown = exception;
            }
        } finally {
            bitmapFile = null;
        }

        try {
            if (dataFile != null) {
                dataFile.close();
            }
        } catch (IOException exception) {
            if (thrown == null) {
                thrown = exception;
            }
        } finally {
            dataFile = null;
        }

        if (thrown != null) {
            throw thrown;
        }
    }

    ///////////
    /// PRIVATE HELPERS
    /// 

    private LongBitMap loadBitmap() throws IOException {
        LongBitMap loadedBitmap = new LongBitMap();
        if (bitmapFile.length() == 0) {
            return loadedBitmap;
        }

        if (bitmapFile.length() % Long.BYTES != 0) {
            throw new IOException("bitmap file length must be a multiple of " + Long.BYTES);
        }

        bitmapFile.seek(0);
        long wordCount = bitmapFile.length() / Long.BYTES;
        for (long wordIndex = 0; wordIndex < wordCount; wordIndex++) {
            long wordValue = bitmapFile.readLong();
            loadedBitmap.setWordValue(wordIndex, wordValue);
        }

        return loadedBitmap;
    }


    private void ensureInitialized() {
        if (dataFile == null || bitmapFile == null || bitmap == null) {
            throw new IllegalStateException("Memory allocator has not been initialized");
        }
    }

    private long requireAllocatedStartBit(long address) throws IOException {
        if (address < 0) {
            throw new IOException("address cannot be negative");
        }
        if (address % BLOCK_SIZE != 0) {
            throw new IOException("address must be aligned to the block size");
        }
        if (!isAllocated(address)) {
            throw new IOException("address does not point to the start of an allocation");
        }

        return address / BLOCK_SIZE;
    }

    private long readStoredLength(long address) throws IOException {
        requireAllocatedStartBit(address);
        long dataLength = readStoredLengthValueAt(address);
        if (dataLength < 0) {
            throw new IOException("stored data length cannot be negative");
        }
        if (address + dataLength + BLOCK_SIZE > dataFile.length()) throw new IOException("somehow the data length goes beyond the file");

        return dataLength;
    }

    private long readStoredLengthValueAt(long address) throws IOException {
        if (address > dataFile.length() - BLOCK_SIZE) {
            throw new IOException("somehow the address is beyond the file");
        }

        dataFile.seek(address);

        byte[] lengthBlock = new byte[(int) BLOCK_SIZE];
        for (int i = 0; i < BLOCK_SIZE; i++) {
            lengthBlock[i] = (byte) dataFile.readUnsignedByte();
        }

        return new LengthBlock(lengthBlock).getLong();
    }

    private void writeStoredLength(long dataLength) throws IOException {
        if (dataLength < 0) {
            throw new IllegalArgumentException("dataLength cannot be negative");
        }
        if (dataLength > Integer.MAX_VALUE) {
            throw new IOException("stored data length exceeds Java array limits");
        }

        dataFile.write(new LengthBlock(dataLength, (int) BLOCK_SIZE).getBytes());
    }

    private long getBlocksNeededForLength(long dataLength) {
        if (dataLength < 0) {
            throw new IllegalArgumentException("dataLength cannot be negative");
        }

        long totalBytesNeeded = BLOCK_SIZE + dataLength;
        return (totalBytesNeeded + BLOCK_SIZE - (long)1) / BLOCK_SIZE;
    }

    // returns next available sequence of bits, returns -1 if it's equivalent to the end of the file (no available space)
    // note that you should account for space for metadata included in blocksNeeded; this function looks for exactly as much space as blocksNeeded
    private long findNextAvailableSequence(long blocksNeeded) {
        ensureInitialized();
        if (blocksNeeded <= 0) {
            throw new IllegalArgumentException("blocksNeeded must be positive");
        }
        
        long start = 0L;
        long end;
        long blocksFound;

        while (true) {
            start = bitmap.nextClearBit(start);
            end = bitmap.nextSetBit(start); // this is what will eventually terminate the loop; it will always get to -1 in the worst case and return the start value

            // if there are no clear bits, return the end of the bitmap
            if (start == -1L) return bitmap.length();

            // if we found a sequence of 0's at the end of the bitmap (regardless of whether there's currently enough space at the end; that is dealt with in create() )
            if (end == -1L) return start;

            // if we found a sequence in the middle that contains enough space
            blocksFound = end - start;
            if (blocksFound >= blocksNeeded) return start;

            // if we didn't find space, let's find the next 0 and start over there.
            start = end + 1;
        }
    }

    private void extendFiles(long numBlocks) throws IOException {
        ensureInitialized();

        if (numBlocks < 0) {
            throw new IllegalArgumentException("numBlocks cannot be negative");
        }
        if (numBlocks == 0) {
            return; // why would this ever happen
        }

        long currentLength = dataFile.length();
        long bytesToAdd = numBlocks * BLOCK_SIZE;
        long newLength = currentLength + bytesToAdd;

        if (newLength < currentLength) {
            throw new IOException("data file length overflow while extending file");  // fascinating
        }

        long currentBlockCapacity = currentLength / BLOCK_SIZE;
        long newBlockCapacity = currentBlockCapacity + numBlocks;
        long newBitmapLength = getBitmapByteLength(newBlockCapacity);

        dataFile.setLength(newLength);
        bitmapFile.setLength(newBitmapLength);
    }

    private long getBitmapByteLength(long blockCount) {
        return getBitmapWordCount(blockCount) * Long.BYTES;
    }

    private long getBitmapWordCount(long blockCount) {
        return (blockCount + Long.SIZE - 1) / Long.SIZE;
    }
}
