package io.github.jbellis.jvector.graph.label;

public interface LabelsSet {

    boolean contains(int label);

    boolean containsAtLeastOne(LabelsSet other);

    int[] get();

 }
