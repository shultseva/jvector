package io.github.jbellis.jvector.graph.label;

import io.github.jbellis.jvector.graph.RandomAccessVectorValues;

public interface RandomAccessVectorLabels<T> {

    int size();

    T vectorLabels(int targetOrd);

    RandomAccessVectorLabels<T> copy();
}
