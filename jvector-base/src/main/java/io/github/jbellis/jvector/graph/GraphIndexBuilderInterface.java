package io.github.jbellis.jvector.graph;

import io.github.jbellis.jvector.disk.RandomAccessReader;
import io.github.jbellis.jvector.graph.label.LabelsSet;
import io.github.jbellis.jvector.graph.label.RandomAccessVectorLabels;

import java.io.IOException;

public interface GraphIndexBuilderInterface<T> {
    OnHeapGraphIndexInterface<T> build();

    void cleanup();

    OnHeapGraphIndexInterface<T> getGraph();

    int insertsInProgress();

    long addGraphNode(int node, RandomAccessVectorValues<T> vectors, RandomAccessVectorLabels<LabelsSet> labels);

    long addGraphNode(int node, RandomAccessVectorValues<T> vectors);

    void improveConnections(int node);

    void markNodeDeleted(int node);

    void load(RandomAccessReader in) throws IOException;
}
