package io.github.jbellis.jvector.graph.label.impl;

import io.github.jbellis.jvector.graph.label.ArraysUtils;
import io.github.jbellis.jvector.graph.label.LabelsSet;

import java.util.Arrays;

@Deprecated
public class LabelsSetAsIntArray implements LabelsSet {

    private final int[] labels;
    public LabelsSetAsIntArray(int[] labels) {
        this.labels = labels;
    }

    @Override
    public boolean contains(int label) {
        var index = Arrays.binarySearch(labels, label);
        return index >= 0;
    }

    @Override
    public boolean containsAtLeastOne(LabelsSet other) {
        return ArraysUtils.hasCommon(this.labels, other.get());
    }

    @Override
    public int[] get() {
        return labels;
    }

    public static LabelsSetAsIntArray asLabelSet(int label) {
        return new LabelsSetAsIntArray(new int[] {label});
    }

    public static LabelsSetAsIntArray asLabelSet(int[] label) {
        return new LabelsSetAsIntArray(label);
    }

}
