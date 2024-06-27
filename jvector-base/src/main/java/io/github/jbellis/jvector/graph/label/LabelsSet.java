package io.github.jbellis.jvector.graph.label;

/**
* set to store labels and perform operation on it
 * */
public interface LabelsSet {

    boolean contains(int label);

    boolean containsAtLeastOne(LabelsSet other);

    LabelsSet intersect(LabelsSet other);

    boolean include(LabelsSet other);

    int[] get();

 }
