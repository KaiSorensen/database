import java.util.InputMismatchException;

public class test {
    public static void main (String[] args) {

        byte[] bytes = {0b00000000, (byte) 0b00000000, 0b00000000, 0b00001000};

        long val = convertBlockToValue(bytes);
        System.out.println(val);

        byte[] newBytes = convertValueToBlock(val, 1);
        System.out.println(bytesToString(newBytes));
        
        long val2 = convertBlockToValue(newBytes);
        System.out.println(val2);

        byte[] bytes2 = convertValueToBlock(val2, 5);
        System.out.println(bytesToString(bytes2));

    }



    private static long convertBlockToValue(byte[] block) {

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

    private static byte[] convertValueToBlock(long value, int blocksize) {
        // or should it be void and just deal with the instance variable?
        // because this will just be called in the constructor
        // answer should be whatever minimizes computation in practice
        // if we create this object, we'll want a conversion. so let's convert in the constructor. but let's make these methods not dependent on the isntance variable in case we want to reuse them later on.
        

        if (blocksize > 8) throw new InputMismatchException("blocks cannot be bigger than a long");

        byte[] block = new byte[blocksize];
        for (int i = blocksize; i > 0; i--) {
            block[i-1] = (byte) (value & 0xFF); // get the 8 bits on the right end (big endian means the least significant bits are on the right)
            value = value >> 8;
        }

        return block;
    }



    private static String bytesToString(byte[] bytes) {
        String s = "";
        
        for(int i = 0; i < bytes.length; i++) {
            s = s + "" + Byte.toString(bytes[i]) + ", ";
        }

        return s;
    }

}
