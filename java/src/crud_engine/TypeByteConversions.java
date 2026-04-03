package crud_engine;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

public class TypeByteConversions {
    private static final int INTEGER_BYTES = Integer.BYTES;
    private static final int BOOLEAN_BYTES = 1;
    private static final int UUID_BYTES = Long.BYTES * 2;

    private TypeByteConversions() {}

    public static byte[] intToBytes(Integer value) {
        return ByteBuffer.allocate(INTEGER_BYTES).putInt(value).array();
    }

    public static int bytesToInt(byte[] bytes) throws IOException {
        requireLength(bytes, INTEGER_BYTES, "INT");
        return ByteBuffer.wrap(bytes).getInt();
    }

    public static byte[] boolToBytes(Boolean value) {
        return new byte[] {(byte) (value ? 1 : 0)};
    }

    public static boolean bytesToBool(byte[] bytes) throws IOException {
        requireLength(bytes, BOOLEAN_BYTES, "BOOL");
        byte value = bytes[0];
        if (value != 0 && value != 1) {
            throw new IOException("Invalid BOOL payload: expected 0 or 1");
        }
        return value == 1;
    }

    public static byte[] stringToBytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    public static String bytesToString(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public static byte[] uuidToBytes(UUID value) {
        return ByteBuffer.allocate(UUID_BYTES)
            .putLong(value.getMostSignificantBits())
            .putLong(value.getLeastSignificantBits())
            .array();
    }

    public static UUID bytesToUuid(byte[] bytes) throws IOException {
        requireLength(bytes, UUID_BYTES, "ID");
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        return new UUID(buffer.getLong(), buffer.getLong());
    }

    private static void requireLength(byte[] bytes, int expected, String typeName) throws IOException {
        if (bytes.length != expected) {
            throw new IOException("Invalid " + typeName + " payload length: expected " + expected);
        }
    }
}
