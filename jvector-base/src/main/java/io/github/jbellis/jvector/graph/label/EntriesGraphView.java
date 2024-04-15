package io.github.jbellis.jvector.graph.label;

import io.github.jbellis.jvector.graph.GraphIndex;

public interface EntriesGraphView<T> extends GraphIndex.View<T> {

    int[] entryNodes();
}
