package io.github.jbellis.jvector.graph.label;

public interface RandomAccessVectorLabels<T> {

    int size();

    T vectorLabels(int targetOrd);

    RandomAccessVectorLabels<T> copy();
}
