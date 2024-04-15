/*
 * All changes to the original code are Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

/*
 * Original license:
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.jbellis.jvector.graph;

import io.github.jbellis.jvector.graph.label.EntriesGraphView;
import io.github.jbellis.jvector.graph.label.LabelConfig;
import io.github.jbellis.jvector.util.Accountable;
import io.github.jbellis.jvector.util.BitSet;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.util.DenseIntMap;
import io.github.jbellis.jvector.util.GrowableBitSet;
import io.github.jbellis.jvector.util.RamUsageEstimator;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.function.BiFunction;
import java.util.stream.IntStream;

/**
 * An {@link GraphIndex} that offers concurrent access; for typical graphs you will get significant
 * speedups in construction and searching as you add threads.
 *
 * <p>To search this graph, you should use a View obtained from {@link #getView()} to perform `seek`
 * and `nextNeighbor` operations.
 */
public class LabeledOnHeapGraphIndex<T> implements OnHeapGraphIndexInterface<T>, Accountable {

    // the current graph entry node on the top level. -1 if not set
    private final AtomicIntegerArray entryPoints;

    private final Map<Integer, Integer> entryToMainLabels = new HashMap<>();
    private final AtomicInteger labelsCount = new AtomicInteger(0);

    private final DenseIntMap<ConcurrentNeighborSet> nodes;
    private final BitSet deletedNodes = new GrowableBitSet(0);
    private final AtomicInteger maxNodeId = new AtomicInteger(-1);

    // max neighbors/edges per node
    final int maxDegree;
    private final BiFunction<Integer, Integer, ConcurrentNeighborSet> neighborFactory;

    LabeledOnHeapGraphIndex(int M, BiFunction<Integer, Integer, ConcurrentNeighborSet> neighborFactory) {
        this.neighborFactory = neighborFactory;
        this.maxDegree = 2 * M;
        this.nodes = new DenseIntMap<>(1024);
        var labels = new int[LabelConfig.MAX_NUMBER_OF_LABELS];
        Arrays.fill(labels, -1);
        this.entryPoints = new AtomicIntegerArray(labels);
    }

    /**
     * Returns the neighbors connected to the given node, or null if the node does not exist.
     *
     * @param node the node whose neighbors are returned, represented as an ordinal on the level 0.
     */
    public ConcurrentNeighborSet getNeighbors(int node) {
        return nodes.get(node);
    }


    @Override
    public int size() {
        return nodes.size();
    }

    /**
     * Add node on the given level with an empty set of neighbors.
     *
     * <p>Nodes can be inserted out of order, but it requires that the nodes preceded by the node
     * inserted out of order are eventually added.
     *
     * <p>Actually populating the neighbors, and establishing bidirectional links, is the
     * responsibility of the caller.
     *
     * <p>It is also the responsibility of the caller to ensure that each node is only added once.
     *
     * @param node the node to add, represented as an ordinal on the level 0.
     * @return the neighbor set for this node
     */
    public ConcurrentNeighborSet addNode(int node) {
        var newNeighborSet = neighborFactory.apply(node, maxDegree());
        nodes.put(node, newNeighborSet);
        maxNodeId.accumulateAndGet(node, Math::max);
        return newNeighborSet;
    }

    /**
     * Only for use by Builder loading a saved graph
     */
    void addNode(int node, ConcurrentNeighborSet neighbors) {
        nodes.put(node, neighbors);
        maxNodeId.accumulateAndGet(node, Math::max);
    }

    /**
     * Mark the given node deleted.  Does NOT remove the node from the graph.
     */
    public void markDeleted(int node) {
        deletedNodes.set(node);
    }

    /**
     * must be called after addNode once neighbors are linked in all levels.
     */
    void maybeSetInitialEntryNode(int node, int label) {
        entryPoints.accumulateAndGet(
                label,
                node,
                (oldEntry, newEntry) -> {
                    if (oldEntry >= 0) {
                        return oldEntry;
                    } else {
                        labelsCount.incrementAndGet();
                        entryToMainLabels.put(newEntry, label);
                        return newEntry;
                    }
                }
        );
    }

    void maybeSetInitialEntryNode(int node) {
        throw new UnsupportedOperationException("maybeSetInitialEntryNode");
    }

    void updateEntryNode(int node, int label) {
        var prev = entryPoints.getAndSet(label, node);
        if (prev < -1) {
            labelsCount.incrementAndGet();
        }
    }

    public void updateEntryNode(int i) {
        throw new UnsupportedOperationException("updateEntryNode");
    }

    @Override
    public int maxDegree() {
        return maxDegree;
    }

    int entry(int label) {
        return entryPoints.get(label);
    }

    int entry() {
        throw new UnsupportedOperationException("entry");
    }

    // return all node ids that are entry point for some label
    int[] entries() {
        // todo races
        int[] nodes = new int[labelsCount.get()];
        int j = 0;
        for (int i = 0; i < LabelConfig.MAX_NUMBER_OF_LABELS; i++) {
            var value = entryPoints.get(i);
            if (value >= 0) {
                nodes[j] = entryPoints.get(i);
                j++;
            }
        }
        return nodes;
    }

    int[] entriesPure() {
        // todo races
        int[] nodes = new int[entryPoints.length()];
        for (int i = 0; i < entryPoints.length(); i++) {
            nodes[i] = entryPoints.get(i);

        }
        return nodes;
    }

    int[] labels() {
        int[] labels = new int[labelsCount.get()];
        int j = 0;
        for (int i = 0; i < LabelConfig.MAX_NUMBER_OF_LABELS; i++) {
            var value = entryPoints.get(i);
            if (value >= 0) {
                labels[j] = i;
                j++;
            }
        }
        return labels;
    }

    @Override
    public NodesIterator getNodes() {
        return nodes.getNodesIterator();
    }

    // TMP
    public Map<Integer, List<Integer>> getGraphRepresentation() {
        Map<Integer, List<Integer>> representation = new HashMap<>();
        var nodesItr = this.getNodes();
        while (nodesItr.hasNext()) {
            var node = nodesItr.next();
            List<Integer> nbrsList = new ArrayList<>();
            var nbrs = this.getNeighbors(node);
            var nbrsIt = nbrs.iterator();
            while (nbrsIt.hasNext()) {
                var nbr = nbrsIt.next();
                nbrsList.add(nbr);
            }
            representation.put(node, nbrsList);
        }
        return representation;
    }

    @Override
    public long ramBytesUsed() {
        // the main graph structure
        long total = (long) size() * RamUsageEstimator.NUM_BYTES_OBJECT_REF;
        long neighborSize = neighborsRamUsed(maxDegree()) * size();
        return total + neighborSize + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;
    }

    public long ramBytesUsedOneNode(int nodeLevel) {
        var graphBytesUsed =
                neighborsRamUsed(maxDegree())
                        + nodeLevel * neighborsRamUsed(maxDegree());
        var clockBytesUsed = Integer.BYTES;
        return graphBytesUsed + clockBytesUsed;
    }

    private static long neighborsRamUsed(int count) {
        long REF_BYTES = RamUsageEstimator.NUM_BYTES_OBJECT_REF;
        long AH_BYTES = RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;
        long neighborSetBytes =
                REF_BYTES // atomicreference
                        + Integer.BYTES
                        + Integer.BYTES
                        + REF_BYTES // NeighborArray
                        + AH_BYTES * 2 // NeighborArray internals
                        + REF_BYTES * 2
                        + Integer.BYTES
                        + 1;
        return neighborSetBytes + (long) count * (Integer.BYTES + Float.BYTES);
    }


    @Override
    public String toString() {
        return String.format("OnHeapGraphIndex(size=%d, entryPoint=%s)", size(), Arrays.toString(entries()));
    }

    @Override
    public void close() {
        // no-op
    }

    /**
     * Returns a view of the graph that is safe to use concurrently with updates performed on the
     * underlying graph.
     *
     * <p>Multiple Views may be searched concurrently.
     */
    @Override
    public EntriesGraphView<T> getView() {
        return new ConcurrentFewEntriesGraphIndexView();
    }

    void validateEntryNode(int label) {
        if (size() == 0) {
            return;
        }
        var en = entryPoints.get(label);
        if (!(en >= 0 && getNeighbors(en) != null)) {
            throw new IllegalStateException("Entry node was incompletely added! " + en);
        }
    }

    void validateEntryNodes() {
        if (size() == 0) {
            return;
        }
        // todo races? may be not a problem
        // todo what we check??
        int completedCounter = 0;
        for (int i = 0; i < entryPoints.length(); i++) {
            var en = entryPoints.get(i);
            if (en >= 0 && getNeighbors(en) != null) { // en < 0 || getNeighbors(en) == null
                completedCounter++;
            }
        }
        if (completedCounter != labelsCount.get()) {
            throw new IllegalStateException("Some entry nodes was incompletely added!");
        }
    }

    public BitSet getDeletedNodes() {
        return deletedNodes;
    }

    /**
     * @return true iff the node was present
     */
    boolean removeNode(int node) {
        return nodes.remove(node) != null;
    }

    @Override
    public int getIdUpperBound() {
        return maxNodeId.get() + 1;
    }

    int[] rawNodes() {
        // it's okay if this is a bit inefficient
        return nodes.keySet().stream().mapToInt(i -> i).toArray();
    }

    public boolean containsNode(int nodeId) {
        return nodes.containsKey(nodeId);
    }

    public double getAverageShortEdges() {
        return IntStream.range(0, getIdUpperBound())
                .filter(this::containsNode)
                .mapToDouble(i -> getNeighbors(i).getShortEdges())
                .average()
                .orElse(Double.NaN);
    }

    public double getAverageDegree() {
        return IntStream.range(0, getIdUpperBound())
                .filter(this::containsNode)
                .mapToDouble(i -> getNeighbors(i).size())
                .average()
                .orElse(Double.NaN);
    }

    private class ConcurrentFewEntriesGraphIndexView implements EntriesGraphView<T> {
        @Override
        public T getVector(int node) {
            throw new UnsupportedOperationException("All searches done with OnHeapGraphIndex should be exact");
        }

        public NodesIterator getNeighborsIterator(int node) {
            return getNeighbors(node).iterator();
        }

        @Override
        public int size() {
            return LabeledOnHeapGraphIndex.this.size();
        }

        @Override
        public int entryNode() {
            return entryPoints.get(0);
        }

        @Override
        public String toString() {
            return "OnHeapGraphIndexView(size=" + size() + ", entryPoint=" + entryPoints;
        }

        @Override
        public Bits liveNodes() {
            // this Bits will return true for node ids that no longer exist in the graph after being purged,
            // but we defined the method contract so that that is okay
            return deletedNodes.cardinality() == 0 ? Bits.ALL : Bits.inverseOf(deletedNodes);
        }

        @Override
        public int getIdUpperBound() {
            return LabeledOnHeapGraphIndex.this.getIdUpperBound();
        }

        @Override
        public void close() {
            // no-op
        }

        @Override
        public int[] entryNodes() {
            // todo races
            int[] nodes = new int[labelsCount.get()];
            int j = 0;
            for (int i = 0; i < LabelConfig.MAX_NUMBER_OF_LABELS; i++) {
                var value = entryPoints.get(i);
                if (value >= 0) {
                    nodes[j] = entryPoints.get(i);
                    j++;
                }
            }
            return nodes;
        }
    }

    public void save(DataOutput out) throws IOException {
        if (deletedNodes.cardinality() > 0) {
            throw new IllegalStateException("Cannot save a graph that has deleted nodes.  Call cleanup() first");
        }

        // graph-level properties
        var view = getView();
        out.writeInt(size());
        out.writeInt(view.entryNode());
        out.writeInt(maxDegree());

        // neighbors
        for (var entry : nodes.entrySet()) {
            var i = (int) (long) entry.getKey();
            var neighbors = entry.getValue().iterator();
            out.writeInt(i);

            out.writeInt(neighbors.size());
            for (int n = 0; n < neighbors.size(); n++) {
                out.writeInt(neighbors.nextInt());
            }
            assert !neighbors.hasNext();
        }
    }
}
