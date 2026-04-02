package memory_allocator;

import java.util.InputMismatchException;

/**
 * The first block of any data stores the length of the data. This class abstracts the conversion of bytes to values, for consistency.
 * We have a variable block size in our database, and we store the length of each data in a whole block
 * Little VS Big Endian matters here
 * We are going with BIG ENDIAN: smallest bit on the right, like how we write it on a white board
 * Our bits will hug the right side.
 */

public class LengthBlock {
    
    private int blocksize;
    private byte[] block;
    private long value;


    public LengthBlock(byte[] lengthBlock) {
        this.block = lengthBlock;
        this.value = convertBlockToValue(this.block);
    }

    public LengthBlock(long lengthValue, int blockSize) {
        this.value = lengthValue;
        this.blocksize = blockSize;
        this.block = convertValueToBlock(this.value, this.blocksize);
    }

    public long getLong() {
        return value;
    }

    public byte[] getBytes() {
        return block;
    }

    private long convertBlockToValue(byte[] block) {

        if (block.length > 8) throw new InputMismatchException("blocks cannot be bigger than a long");

        long val = 0;
        for (int i = 0; i < block.length; i++) {
            // shift val to the left by 1 byte
            val = val << 8;
            // insert the 1 bits with an 'or' operation
            val = val | block[i];
        }

        return val;
    }

    private byte[] convertValueToBlock(long value, int sizeOfBlock) {
        // or should it be void and just deal with the instance variable?
        // because this will just be called in the constructor
        // answer should be whatever minimizes computation in practice
        // if we create this object, we'll want a conversion. so let's convert in the constructor. but let's make these methods not dependent on the isntance variable in case we want to reuse them later on.
        
        if (sizeOfBlock > 8) throw new InputMismatchException("blocks cannot be bigger than a long");

        byte[] block = new byte[sizeOfBlock];
        for (int i = sizeOfBlock; i > 0; i--) {
            block[i-1] = (byte) (value & 0xFF); // get the 8 bits on the right end (big endian means the least significant bits are on the right)
            value = value >> 8;
        }

        return block;
    }

}
