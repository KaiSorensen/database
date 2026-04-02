package memory_allocator;

import java.util.NavigableMap;
import java.util.TreeMap;

// AI-GENERATED DATA STRUCTURE FOR LONG BITMAPS
// We need a bitmap that can be a variable length

/**
 * In-memory bitmap that supports long bit indexes.
 *
 * <p>The bitmap is stored sparsely as 64-bit words. Missing words are treated as all zeroes.</p>
 */
public class LongBitMap {
    private static final int BITS_PER_WORD = Long.SIZE;

    private final NavigableMap<Long, Long> words = new TreeMap<>();

    public boolean get(long bitIndex) {
        validateBitIndex(bitIndex);

        long word = getWord(getWordIndex(bitIndex));
        long mask = 1L << getBitOffset(bitIndex);
        return (word & mask) != 0;
    }

    public void set(long bitIndex) {
        validateBitIndex(bitIndex);

        long wordIndex = getWordIndex(bitIndex);
        long word = getWord(wordIndex);
        word |= 1L << getBitOffset(bitIndex);
        putWord(wordIndex, word);
    }

    public void clear(long bitIndex) {
        validateBitIndex(bitIndex);

        long wordIndex = getWordIndex(bitIndex);
        long word = getWord(wordIndex);
        word &= ~(1L << getBitOffset(bitIndex));
        putWord(wordIndex, word);
    }

    public void set(long fromInclusive, long toExclusive) {
        validateRange(fromInclusive, toExclusive);
        if (fromInclusive == toExclusive) {
            return;
        }

        long startWordIndex = getWordIndex(fromInclusive);
        long endWordIndex = getWordIndex(toExclusive - 1);
        int startOffset = getBitOffset(fromInclusive);
        int endOffsetExclusive = getBitOffset(toExclusive - 1) + 1;

        if (startWordIndex == endWordIndex) {
            long word = getWord(startWordIndex);
            word |= buildMask(startOffset, endOffsetExclusive);
            putWord(startWordIndex, word);
            return;
        }

        long firstWord = getWord(startWordIndex);
        firstWord |= buildMask(startOffset, BITS_PER_WORD);
        putWord(startWordIndex, firstWord);

        for (long wordIndex = startWordIndex + 1; wordIndex < endWordIndex; wordIndex++) {
            putWord(wordIndex, -1L);
        }

        long lastWord = getWord(endWordIndex);
        lastWord |= buildMask(0, endOffsetExclusive);
        putWord(endWordIndex, lastWord);
    }

    public void clear(long fromInclusive, long toExclusive) {
        validateRange(fromInclusive, toExclusive);
        if (fromInclusive == toExclusive) {
            return;
        }

        long startWordIndex = getWordIndex(fromInclusive);
        long endWordIndex = getWordIndex(toExclusive - 1);
        int startOffset = getBitOffset(fromInclusive);
        int endOffsetExclusive = getBitOffset(toExclusive - 1) + 1;

        if (startWordIndex == endWordIndex) {
            long word = getWord(startWordIndex);
            word &= ~buildMask(startOffset, endOffsetExclusive);
            putWord(startWordIndex, word);
            return;
        }

        long firstWord = getWord(startWordIndex);
        firstWord &= ~buildMask(startOffset, BITS_PER_WORD);
        putWord(startWordIndex, firstWord);

        for (long wordIndex = startWordIndex + 1; wordIndex < endWordIndex; wordIndex++) {
            words.remove(wordIndex);
        }

        long lastWord = getWord(endWordIndex);
        lastWord &= ~buildMask(0, endOffsetExclusive);
        putWord(endWordIndex, lastWord);
    }

    public long nextClearBit(long fromIndex) {
        validateBitIndex(fromIndex);

        long wordIndex = getWordIndex(fromIndex);
        int bitOffset = getBitOffset(fromIndex);

        while (true) {
            long word = getWord(wordIndex);
            if (bitOffset > 0) {
                word |= (1L << bitOffset) - 1L;
            }

            if (~word != 0L) {
                return wordIndex * BITS_PER_WORD + Long.numberOfTrailingZeros(~word);
            }

            Long nextWordIndex = words.higherKey(wordIndex);
            if (nextWordIndex == null || nextWordIndex > wordIndex + 1) {
                return (wordIndex + 1) * BITS_PER_WORD;
            }

            wordIndex = nextWordIndex;
            bitOffset = 0;
        }
    }

    public long nextSetBit(long fromIndex) {
        validateBitIndex(fromIndex);

        long wordIndex = getWordIndex(fromIndex);
        int bitOffset = getBitOffset(fromIndex);

        while (true) {
            long word = getWord(wordIndex);
            if (bitOffset > 0) {
                word &= ~((1L << bitOffset) - 1L);
            }

            if (word != 0L) {
                return wordIndex * BITS_PER_WORD + Long.numberOfTrailingZeros(word);
            }

            Long nextWordIndex = words.higherKey(wordIndex);
            if (nextWordIndex == null) {
                return -1L;
            }

            wordIndex = nextWordIndex;
            bitOffset = 0;
        }
    }

    public boolean isEmpty() {
        return words.isEmpty();
    }

    public long length() {
        if (words.isEmpty()) {
            return 0L;
        }

        long lastWordIndex = words.lastKey();
        long lastWordValue = words.get(lastWordIndex);
        return lastWordIndex * BITS_PER_WORD + (BITS_PER_WORD - Long.numberOfLeadingZeros(lastWordValue));
    }

    long getWordValue(long wordIndex) {
        if (wordIndex < 0) {
            throw new IllegalArgumentException("wordIndex cannot be negative");
        }
        return getWord(wordIndex);
    }

    void setWordValue(long wordIndex, long value) {
        if (wordIndex < 0) {
            throw new IllegalArgumentException("wordIndex cannot be negative");
        }
        putWord(wordIndex, value);
    }

    public void clear() {
        words.clear();
    }

    private long getWord(long wordIndex) {
        return words.getOrDefault(wordIndex, 0L);
    }

    private void putWord(long wordIndex, long value) {
        if (value == 0L) {
            words.remove(wordIndex);
        } else {
            words.put(wordIndex, value);
        }
    }

    private long getWordIndex(long bitIndex) {
        return bitIndex / BITS_PER_WORD;
    }

    private int getBitOffset(long bitIndex) {
        return (int) (bitIndex % BITS_PER_WORD);
    }

    private long buildMask(int fromInclusive, int toExclusive) {
        long startMask = -1L << fromInclusive;
        long endMask = toExclusive == BITS_PER_WORD ? -1L : (1L << toExclusive) - 1L;
        return startMask & endMask;
    }

    private void validateBitIndex(long bitIndex) {
        if (bitIndex < 0) {
            throw new IllegalArgumentException("bitIndex cannot be negative");
        }
    }

    private void validateRange(long fromInclusive, long toExclusive) {
        validateBitIndex(fromInclusive);
        validateBitIndex(toExclusive);
        if (toExclusive < fromInclusive) {
            throw new IllegalArgumentException("toExclusive cannot be less than fromInclusive");
        }
    }
}
