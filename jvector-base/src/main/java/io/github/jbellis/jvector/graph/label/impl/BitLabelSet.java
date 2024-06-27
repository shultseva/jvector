package io.github.jbellis.jvector.graph.label.impl;

import io.github.jbellis.jvector.graph.label.LabelsSet;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Objects;

public class BitLabelSet implements LabelsSet {

    protected final BitSet bitSet;

    public BitLabelSet(int[] labels) {
        this.bitSet = new BitSet(labels.length);
        for (var label : labels) {
            bitSet.set(label);
        }
    }

    private BitLabelSet(BitSet other) {
        this.bitSet = other;
    }

    @Override
    public boolean contains(int label) {
        return bitSet.get(label);
    }

    @Override
    public boolean containsAtLeastOne(LabelsSet other) {
        if (other instanceof BitLabelSet) {
            var otherBitSet = (BitLabelSet) other;
            return bitSet.intersects(otherBitSet.bitSet);
        }
        return false;
    }

    @Override
    public LabelsSet intersect(LabelsSet other) {
        var copy = (BitSet) bitSet.clone();
        if (other instanceof BitLabelSet) {
            var otherBitSet = (BitLabelSet) other;
            copy.and(otherBitSet.bitSet);
            return new BitLabelSet(copy);
        }
        throw new RuntimeException("");
    }

    @Override
    public boolean include(LabelsSet other) {
        var intersect = this.intersect(other);
        return other.equals(intersect);
    }

    @Override
    public int[] get() {
        int[] array = new int[bitSet.size()];
        int index = 0;
        for (int i = bitSet.nextSetBit(0); i < bitSet.length(); i = bitSet.nextSetBit(i + 1)) {
            if (i == -1) {
                break;
            }
            array[index] = i;
            index++;
        }
        return Arrays.copyOf(array, index);
    }

    public static LabelsSet asLabelSet(int label) {
        return new BitLabelSet(new int[] {label});
    }

    public static LabelsSet asLabelSet(int[] label) {
        return new BitLabelSet(label);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        BitLabelSet that = (BitLabelSet) object;
        return Objects.equals(bitSet, that.bitSet);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bitSet);
    }
}
