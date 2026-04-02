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
public class BitmapMemoryAllocator implements MemoryAllocator {
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

        // add one block size for storing the length of the data as metadata
        long totalBytesNeeded = BLOCK_SIZE + (long) data.length;
        // add block_size - 1 because we want it to round up to the nearest block if there is a fractional remainder at the end of the data
        long blocksNeeded = (totalBytesNeeded +  BLOCK_SIZE - (long)1) / BLOCK_SIZE;

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
        // write the data at that location
        dataFile.writeInt(data.length);
        dataFile.write(data);
        // update the bitmap to reflect the presence of the new data
        bitmap.set(startBit, startBit + blocksNeeded);

        // return the byte index (address) of the stored data so that it can be stored as the object/attribute pairing
        return startByte;
    }

    @Override
    public byte[] read(long address) throws IOException {
        ensureInitialized();

        if (address > dataFile.length() - BLOCK_SIZE) throw new IOException("somehow the address is beyond the file");
        // if somehow the specified data length is beyond the file

        dataFile.seek(address);

        byte[] lengthBlock = new byte[(int) BLOCK_SIZE]; // this cast is always okay because block_size can't be more than 8
        for (int i = 0; i < BLOCK_SIZE; i++) {
            lengthBlock[i] = dataFile.readByte();
        }
        LengthBlock dataLengthBytes = new LengthBlock(lengthBlock);
        long datalength = dataLengthBytes.getLong();

        if (address + datalength + BLOCK_SIZE > dataFile.length()) throw new IOException("somehow the data length goes beyond the file");


        /** NOTE: THE FUNDAMENTAL DATA LENGTH LIMITATION COMES FROM JAVA ARRAY INDEXING */  
        byte[] data = new byte[(int) datalength]; // another fundamental limitation: you can't have a java array bigger than an integer?
        for (int i = 0; i < datalength; i++) {
            data[i] = dataFile.readByte();
        }

        return data;
    }

    @Override
    public long update(long address, byte[] newData) throws IOException {
        throw new UnsupportedOperationException("update is not implemented yet");
    }

    @Override
    public void delete(long address) throws IOException {
        throw new UnsupportedOperationException("delete is not implemented yet");
    }

    @Override
    public boolean isAllocated(long address) throws IOException {
        throw new UnsupportedOperationException("isAllocated is not implemented yet");
    }

    @Override
    public int getLength(long address) throws IOException {
        throw new UnsupportedOperationException("getLength is not implemented yet");
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
            if (bitmapFile != null || dataFile != null) {
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
