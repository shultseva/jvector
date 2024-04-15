package io.github.jbellis.jvector.graph.label;

import java.util.Arrays;
import java.util.Objects;

public class MutableAccessVectorLabels implements RandomAccessVectorLabels<LabelsSet>{

    int MAX_NUMBER_OF_NODES = 100_000;
    Int2SortedIntArrayHashMap vectorLabelsMap = new Int2SortedIntArrayHashMap(MAX_NUMBER_OF_NODES, 0.9);

    public MutableAccessVectorLabels(Int2SortedIntArrayHashMap vectorLabelsMap) {
        this.vectorLabelsMap = vectorLabelsMap;
    }

    public MutableAccessVectorLabels() {
    }

    @Override
    public int size() {
        return vectorLabelsMap.getSize();
    }

    @Override
    public LabelsSet vectorLabels(int targetOrd) {
        return new LabelsSetAsIntArray(vectorLabelsMap.get(targetOrd));
    }

    public static LabelsSetAsIntArray singletonLabelSet(int label) {
        return new LabelsSetAsIntArray(new int[] {label});
    }

    public static LabelsSetAsIntArray asLabelSet(int[] label) {
        return new LabelsSetAsIntArray(label);
    }

    @Override
    public RandomAccessVectorLabels<LabelsSet> copy() {
        return new MutableAccessVectorLabels(this.vectorLabelsMap);
    }

    public void put(int node, int[] labels) {
        vectorLabelsMap.put(node, labels);
    }

    public static class LabelsSetAsIntArray implements LabelsSet {

        private final int[] labels;
        private LabelsSetAsIntArray(int[] labels) {
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

        @Override
        public int get(int i) {
            Objects.checkIndex(i, labels.length);
            return labels[i];
        }
    }
}
