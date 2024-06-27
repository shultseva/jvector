package io.github.jbellis.jvector.graph.label;

import io.github.jbellis.jvector.graph.label.impl.BitLabelSet;
import io.github.jbellis.jvector.graph.label.impl.Int2SortedIntArrayHashMap;

public class MutableAccessVectorLabels implements RandomAccessVectorLabels<LabelsSet>{

    int INITIAL_NUMBER_OF_NODES = 100_000;

    //todo Int2BitSet
    Int2SortedIntArrayHashMap vectorLabelsMap = new Int2SortedIntArrayHashMap(INITIAL_NUMBER_OF_NODES, 0.9);

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
        return BitLabelSet.asLabelSet(vectorLabelsMap.get(targetOrd));
    }

    @Override
    public RandomAccessVectorLabels<LabelsSet> copy() {
        return new MutableAccessVectorLabels(this.vectorLabelsMap);
    }

    public void put(int node, int[] labels) {
        vectorLabelsMap.put(node, labels);
    }

}
