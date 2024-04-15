package io.github.jbellis.jvector.graph.label.impl;

import io.github.jbellis.jvector.graph.label.LabelsSet;

import java.util.Arrays;
import java.util.BitSet;

public class BitLabelSet implements LabelsSet {

    private final BitSet bitSet;

    public BitLabelSet(int[] labels) {
        this.bitSet = new BitSet(labels.length);
        for (var label : labels) {
            bitSet.set(label);
        }
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
}
