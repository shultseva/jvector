package io.github.jbellis.jvector.graph.label;

import java.util.Arrays;

import static java.util.Objects.requireNonNull;
import static org.apache.commons.math3.util.MathUtils.checkNotNull;

public class Int2SortedIntArrayHashMap {
    /**
     * The default load factor for constructors not explicitly supplying it.
     */
    public static final double DEFAULT_LOAD_FACTOR = 0.6;
    /**
     * The default initial capacity for constructors not explicitly supplying it.
     */
    public static final int DEFAULT_INITIAL_CAPACITY = 8;
    private final int[] nullValue;
    private int capacity;
    private int resizeThreshold;

    private int mask;

    private int size;

    private final double loadFactor;

    private int[] keys;
    private int[][] values;


    public Int2SortedIntArrayHashMap(final int initialCapacity, final double loadFactor) {
        this.loadFactor = loadFactor;
        this.nullValue = new int[0];
        this.capacity = nextPowerOfTwo(initialCapacity);
        mask = capacity - 1;
        this.resizeThreshold = (int) (capacity * loadFactor);

        keys = new int[capacity];
        values = new int[capacity][0];
    }

    public int getSize() {
        return size;
    }

    private int nextPowerOfTwo(int value) {
        return 1 << 32 - Integer.numberOfLeadingZeros(value - 1);
    }

    public int[] get(final int key) {
        int index = intHash(key, mask);
        int[] value;
        while (nullValue != (value = values[index])) {
            if (key == keys[index]) {
                return value;
            }
            index = ++index & mask;
        }
        return null;
    }

    public int[] put(final int key, final int[] value) {
        requireNonNull(value, "Value cannot be null");
        Arrays.sort(value);
        int[] oldValue = null;
        int index = intHash(key, mask);
        while (!Arrays.equals(nullValue, values[index])) {
            if (key == keys[index]) {
                oldValue = values[index];
                break;
            }
            index = ++index & mask;
        }
        if (null == oldValue) {
            ++size;
            keys[index] = key;
        }
        values[index] = value;
        if (size > resizeThreshold) {
            increaseCapacity();
        }
        return (oldValue == nullValue) ? null : oldValue;
    }

    public static int intHash(int value, int mask) {
        return fastIntMix(value) & mask;
    }

    private static int fastIntMix(int k) {
        // phi = 2^32 / goldenRatio
        final int phi = 0x9E3779B9;
        final int h = k * phi;
        return h ^ (h >>> 16);
    }


    private void increaseCapacity() {
        final int newCapacity = capacity << 1;
        if (newCapacity < 0) {
            throw new IllegalStateException("Max capacity reached at size=" + size);
        }
        rehash(newCapacity);
    }


    private void rehash(final int newCapacity) {
        if (1 != Integer.bitCount(newCapacity)) {
            throw new IllegalStateException("New capacity must be a power of two");
        }
        capacity = newCapacity;
        mask = newCapacity - 1;
        resizeThreshold = (int) (newCapacity * loadFactor);
        final int[] tempKeys = new int[capacity];
        final int[][] tempValues = new int[capacity][0];;
        initializeValues(tempValues);
        for (int i = 0, size = values.length; i < size; i++) {
            final int[] value = values[i];
            if (nullValue != value) {
                final int key = keys[i];
                int newHash = intHash(key, mask);
                while (nullValue != tempValues[newHash]) {
                    newHash = ++newHash & mask;
                }
                tempKeys[newHash] = key;
                tempValues[newHash] = value;
            }
        }
        keys = tempKeys;
        values = tempValues;
    }


    private void initializeValues(int[][] values) {
        for (int i = 0; i < capacity; i++) {
            values[i] = nullValue;
        }
    }
}
