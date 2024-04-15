package io.github.jbellis.jvector.graph;

import java.io.DataOutput;
import java.io.IOException;

public interface OnHeapGraphIndexInterface<T> extends GraphIndex<T>{
    ConcurrentNeighborSet getNeighbors(int node);
    void save(DataOutput out) throws IOException;
}
