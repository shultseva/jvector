/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.jbellis.jvector.graph;

import io.github.jbellis.jvector.annotations.VisibleForTesting;
import io.github.jbellis.jvector.disk.RandomAccessReader;
import io.github.jbellis.jvector.graph.label.LabelsSet;
import io.github.jbellis.jvector.graph.label.RandomAccessVectorLabels;
import io.github.jbellis.jvector.util.AtomicFixedBitSet;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.util.PhysicalCoreExecutor;
import io.github.jbellis.jvector.util.PoolingSupport;
import io.github.jbellis.jvector.vector.VectorEncoding;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.VectorUtil;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static io.github.jbellis.jvector.util.DocIdSetIterator.NO_MORE_DOCS;

/**
 * Builder for Concurrent GraphIndex. See {@link GraphIndex} for a high level overview, and the
 * comments to `addGraphNode` for details on the concurrent building approach.
 *
 * @param <T> the type of vector
 */
public class GraphIndexBuilder<T> implements GraphIndexBuilderInterface<T> {
    private final int beamWidth;
    private final PoolingSupport<NodeArray> naturalScratch;
    private final PoolingSupport<NodeArray> concurrentScratch;

    private final VectorSimilarityFunction similarityFunction;
    private final float neighborOverflow;
    private final float alpha;
    private final VectorEncoding vectorEncoding;
    private final PoolingSupport<GraphSearcher<?>> graphSearcher;

    @VisibleForTesting
    final OnHeapGraphIndex<T> graph;
    private final ConcurrentSkipListSet<Integer> insertionsInProgress = new ConcurrentSkipListSet<>();

    // We need two sources of vectors in order to perform diversity check comparisons without
    // colliding.  Usually it's obvious because you can see the different sources being used
    // in the same method.  The only tricky place is in addGraphNode, which uses `vectors` immediately,
    // and `vectorsCopy` later on when defining the ScoreFunction for search.
    private final PoolingSupport<RandomAccessVectorValues<T>> vectors;
    private final PoolingSupport<RandomAccessVectorValues<T>> vectorsCopy;
    private final int dimension; // for convenience so we don't have to go to the pool for this
    private final NodeSimilarity similarity;

    private final ForkJoinPool simdExecutor;
    private final ForkJoinPool parallelExecutor;

    private final AtomicInteger updateEntryNodeIn = new AtomicInteger(10_000);

    /**
     * Reads all the vectors from vector values, builds a graph connecting them by their dense
     * ordinals, using the given hyperparameter settings, and returns the resulting graph.
     *
     * @param vectorValues     the vectors whose relations are represented by the graph - must provide a
     *                         different view over those vectors than the one used to add via addGraphNode.
     * @param M                – the maximum number of connections a node can have
     * @param beamWidth        the size of the beam search to use when finding nearest neighbors.
     * @param neighborOverflow the ratio of extra neighbors to allow temporarily when inserting a
     *                         node. larger values will build more efficiently, but use more memory.
     * @param alpha            how aggressive pruning diverse neighbors should be.  Set alpha &gt; 1.0 to
     *                         allow longer edges.  If alpha = 1.0 then the equivalent of the lowest level of
     *                         an HNSW graph will be created, which is usually not what you want.
     */
    public GraphIndexBuilder(
            RandomAccessVectorValues<T> vectorValues,
            VectorEncoding vectorEncoding,
            VectorSimilarityFunction similarityFunction,
            int M,
            int beamWidth,
            float neighborOverflow,
            float alpha) {
        this(vectorValues, vectorEncoding, similarityFunction, M, beamWidth, neighborOverflow, alpha,
                PhysicalCoreExecutor.pool(), ForkJoinPool.commonPool());
    }

    /**
     * Reads all the vectors from vector values, builds a graph connecting them by their dense
     * ordinals, using the given hyperparameter settings, and returns the resulting graph.
     *
     * @param vectorValues     the vectors whose relations are represented by the graph - must provide a
     *                         different view over those vectors than the one used to add via addGraphNode.
     * @param M                – the maximum number of connections a node can have
     * @param beamWidth        the size of the beam search to use when finding nearest neighbors.
     * @param neighborOverflow the ratio of extra neighbors to allow temporarily when inserting a
     *                         node. larger values will build more efficiently, but use more memory.
     * @param alpha            how aggressive pruning diverse neighbors should be.  Set alpha &gt; 1.0 to
     *                         allow longer edges.  If alpha = 1.0 then the equivalent of the lowest level of
     *                         an HNSW graph will be created, which is usually not what you want.
     * @param simdExecutor     ForkJoinPool instance for SIMD operations, best is to use a pool with the size of
     *                         the number of physical cores.
     * @param parallelExecutor ForkJoinPool instance for parallel stream operations
     */
    public GraphIndexBuilder(
            RandomAccessVectorValues<T> vectorValues,
            VectorEncoding vectorEncoding,
            VectorSimilarityFunction similarityFunction,
            int M,
            int beamWidth,
            float neighborOverflow,
            float alpha,
            ForkJoinPool simdExecutor,
            ForkJoinPool parallelExecutor) {
        vectors = vectorValues.isValueShared() ? PoolingSupport.newThreadBased(vectorValues::copy) : PoolingSupport.newNoPooling(vectorValues);
        vectorsCopy = vectorValues.isValueShared() ? PoolingSupport.newThreadBased(vectorValues::copy) : PoolingSupport.newNoPooling(vectorValues);
        dimension = vectorValues.dimension();
        this.vectorEncoding = Objects.requireNonNull(vectorEncoding);
        this.similarityFunction = Objects.requireNonNull(similarityFunction);
        this.neighborOverflow = neighborOverflow;
        this.alpha = alpha;
        if (M <= 0) {
            throw new IllegalArgumentException("maxConn must be positive");
        }
        if (beamWidth <= 0) {
            throw new IllegalArgumentException("beamWidth must be positive");
        }
        this.beamWidth = beamWidth;
        this.simdExecutor = simdExecutor;
        this.parallelExecutor = parallelExecutor;

        similarity = node1 -> {
            try (var v = vectors.get(); var vc = vectorsCopy.get()) {
                T v1 = v.get().vectorValue(node1);
                return (NodeSimilarity.ExactScoreFunction) node2 -> scoreBetween(v1, vc.get().vectorValue(node2));
            }
        };
        this.graph =
                new OnHeapGraphIndex<>(
                        M, (node, m) -> new ConcurrentNeighborSet(node, m, similarity, alpha));

        this.graphSearcher = PoolingSupport.newThreadBased(() -> new GraphSearcher.Builder<>(graph.getView()).withConcurrentUpdates().build());

        // in scratch we store candidates in reverse order: worse candidates are first
        this.naturalScratch = PoolingSupport.newThreadBased(() -> new NodeArray(Math.max(beamWidth, M + 1)));
        this.concurrentScratch = PoolingSupport.newThreadBased(() -> new NodeArray(Math.max(beamWidth, M + 1)));
    }

    public OnHeapGraphIndex<T> build() {
        int size;
        try (var v = vectors.get()) {
            size = v.get().size();
        }

        simdExecutor.submit(() -> {
            IntStream.range(0, size).parallel().forEach(i -> {
                try (var v1 = vectors.get()) {
                    addGraphNode(i, v1.get());
                }
            });
        }).join();

        cleanup();
        return graph;
    }

    /**
     * Cleanup the graph by completing removal of marked-for-delete nodes, trimming
     * neighbor sets to the advertised degree, and updating the entry node.
     * <p>
     * Uses default threadpool to process nodes in parallel.  There is currently no way to restrict this to a single thread.
     * <p>
     * Must be called before writing to disk.
     * <p>
     * May be called multiple times, but should not be called during concurrent modifications to the graph.
     */
    public void cleanup() {
        if (graph.size() == 0) {
            return;
        }
        graph.validateEntryNode(); // sanity check before we start

        // purge deleted nodes.
        // backlinks can cause neighbors to soft-overflow, so do this before neighbors cleanup
        removeDeletedNodes();

        // clean up overflowed neighbor lists
        parallelExecutor.submit(() -> IntStream.range(0, graph.getIdUpperBound()).parallel().forEach(i -> {
            var neighbors = graph.getNeighbors(i);
            if (neighbors != null) {
                neighbors.cleanup();
            }
        })).join();

        if (graph.size() > 0) {
            // reconnect any orphaned nodes.  this will maintain neighbors size
            reconnectOrphanedNodes();
            // optimize entry node
            graph.updateEntryNode(approximateMedioid());
            updateEntryNodeIn.set(graph.size()); // in case the user goes on to add more nodes after cleanup()
        } else {
            updateEntryNodeIn.set(10_000); // default value for empty graph
        }

    }

    private void reconnectOrphanedNodes() {
        // It's possible that reconnecting one node will result in disconnecting another, since we are maintaining
        // the maxConnections invariant.  In an extreme case, reconnecting node X disconnects Y, and reconnecting
        // Y disconnects X again.  So we do a best effort of 3 loops.
        for (int i = 0; i < 3; i++) {
            // find all nodes reachable from the entry node
            var connectedNodes = new AtomicFixedBitSet(graph.getIdUpperBound());
            connectedNodes.set(graph.entry());
            var entryNeighbors = graph.getNeighbors(graph.entry()).getCurrent();
            parallelExecutor.submit(() -> IntStream.range(0, entryNeighbors.size).parallel().forEach(node -> {
                findConnected(connectedNodes, entryNeighbors.node[node]);
            })).join();

            // reconnect unreachable nodes
            var nReconnected = new AtomicInteger();
            try (var gs = graphSearcher.get();
                 var v1 = vectors.get();
                 var v2 = vectorsCopy.get())
            {
                var connectionTargets = new HashSet<Integer>();
                for (int node = 0; node < graph.getIdUpperBound(); node++) {
                    if (!connectedNodes.get(node) && graph.containsNode(node)) {
                        // search for the closest neighbors
                        var notSelfBits = createNotSelfBits(node);
                        var value = v1.get().vectorValue(node);
                        NodeSimilarity.ExactScoreFunction scoreFunction = i1 -> scoreBetween(v2.get().vectorValue(i1), value);
                        var result = gs.get().searchInternal(scoreFunction, null, beamWidth, 0.0f, graph.entry(), notSelfBits).getNodes();
                        // connect this node to the closest neighbor that hasn't already been used as a connection target
                        // (since this edge is likely to be the "worst" one in that target's neighborhood, it's likely to be
                        // overwritten by the next node to need reconnection if we don't enforce uniqueness)
                        for (var ns : result) {
                            if (connectionTargets.add(ns.node)) {
                                graph.getNeighbors(ns.node).insertNotDiverse(node, ns.score, true);
                                break;
                            }
                        }
                        nReconnected.incrementAndGet();
                    }
                }
            }
            if (nReconnected.get() == 0) {
                break;
            }
        }
    }

    private void findConnected(AtomicFixedBitSet connectedNodes, int start) {
        var queue = new ArrayDeque<Integer>();
        queue.add(start);
        var view = graph.getView();
        while (!queue.isEmpty()) {
            // DFS should result in less contention across findConnected threads than BFS
            int next = queue.pop();
            if (connectedNodes.getAndSet(next)) {
                continue;
            }
            for (var it = view.getNeighborsIterator(next); it.hasNext(); ) {
                queue.add(it.nextInt());
            }
        }
    }

    public OnHeapGraphIndex<T> getGraph() {
        return graph;
    }

    /**
     * Number of inserts in progress, across all threads.
     */
    public int insertsInProgress() {
        return insertionsInProgress.size();
    }

    @Override
    public long addGraphNode(int node, RandomAccessVectorValues<T> vectors, RandomAccessVectorLabels<LabelsSet> labels) {
        throw new UnsupportedOperationException("Use labeled indexed!");
    }

    /**
     * Inserts a node with the given vector value to the graph.
     *
     * <p>To allow correctness under concurrency, we track in-progress updates in a
     * ConcurrentSkipListSet. After adding ourselves, we take a snapshot of this set, and consider all
     * other in-progress updates as neighbor candidates.
     *
     * @param node    the node ID to add
     * @param vectors the set of vectors
     * @return an estimate of the number of extra bytes used by the graph after adding the given node
     */
    public long addGraphNode(int node, RandomAccessVectorValues<T> vectors) {
        final T value = vectors.vectorValue(node);

        // do this before adding to in-progress, so a concurrent writer checking
        // the in-progress set doesn't have to worry about uninitialized neighbor sets
        var newNodeNeighbors = graph.addNode(node);

        insertionsInProgress.add(node);
        ConcurrentSkipListSet<Integer> inProgressBefore = insertionsInProgress.clone();
        try (var gs = graphSearcher.get();
             var vc = vectorsCopy.get();
             var naturalScratchPooled = naturalScratch.get();
             var concurrentScratchPooled = concurrentScratch.get())
        {
            // find ANN of the new node by searching the graph
            int ep = graph.entry();
            NodeSimilarity.ExactScoreFunction scoreFunction = i -> scoreBetween(vc.get().vectorValue(i), value);

            var bits = new ExcludingBits(node);
            // find best "natural" candidates with a beam search
            var result = gs.get().searchInternal(scoreFunction, null, beamWidth, 0.0f, ep, bits);

            // Update neighbors with these candidates.
            // The DiskANN paper calls for using the entire set of visited nodes along the search path as
            // potential candidates, but in practice we observe neighbor lists being completely filled using
            // just the topK results.  (Since the Robust Prune algorithm prioritizes closer neighbors,
            // this means that considering additional nodes from the search path, that are by definition
            // farther away than the ones in the topK, would not change the result.)
            // TODO if we made NeighborArray an interface we could wrap the NodeScore[] directly instead of copying
            // найденные ближайшие узлы в отсортированном виде
            var natural = toScratchCandidates(result.getNodes(), result.getNodes().length, naturalScratchPooled.get());
            // узлы которые сейчас находятся в процессе добавление в отсортированном виде
            var concurrent = getConcurrentCandidates(node, inProgressBefore, concurrentScratchPooled.get(), vectors, vc.get());
            updateNeighbors(newNodeNeighbors, natural, concurrent);

            maybeUpdateEntryPoint(node);
            maybeImproveOlderNode();
        } finally {
            insertionsInProgress.remove(node);
        }

        return graph.ramBytesUsedOneNode(0);
    }

    /**
     * Improve edge quality on very low-d indexes.  This makes a big difference
     * in the ability of search to escape local maxima to find better options.
     * <p>
     * This has negligible effect on ML embedding-sized vectors, starting at least with GloVe-25, so we don't bother.
     * (Dimensions between 4 and 25 are untested but they get left out too.)
     * For 2D vectors, this takes us to over 99% recall up to at least 4M nodes.  (Higher counts untested.)
    */
    private void maybeImproveOlderNode() {
        // pick a node added earlier at random to improve its connections
        // 20k threshold chosen because that's where recall starts to fall off from 100% for 2D vectors
        if (dimension <= 3 && graph.size() > 20_000) {
            // if we can't find a candidate in 3 tries, the graph is too sparse,
            // we'll have to wait for more nodes to be added (this threshold has been tested w/ parallel build,
            // which generates very sparse ids due to how spliterator works)
            for (int i = 0; i < 3; i++) {
                var olderNode = ThreadLocalRandom.current().nextInt(graph.size());
                if (graph.containsNode(olderNode)) {
                    improveConnections(olderNode);
                    break;
                }
            }
        }
    }

    private void maybeUpdateEntryPoint(int node) {
        graph.maybeSetInitialEntryNode(node); // TODO it seems silly to call this long after we've set it the first time

        if (updateEntryNodeIn.decrementAndGet() == 0) {
            int newEntryNode = approximateMedioid();
            graph.updateEntryNode(newEntryNode);
            improveConnections(newEntryNode);
            updateEntryNodeIn.addAndGet(graph.size());
        }
    }

    public void improveConnections(int node) {
        try (var pv = vectors.get();
             var gs = graphSearcher.get();
             var vc = vectorsCopy.get();
             var naturalScratchPooled = naturalScratch.get())
        {
            final T value = pv.get().vectorValue(node);

            // find ANN of the new node by searching the graph
            int ep = graph.entry();
            NodeSimilarity.ExactScoreFunction scoreFunction = i -> scoreBetween(vc.get().vectorValue(i), value);
            var bits = new ExcludingBits(node);
            var result = gs.get().searchInternal(scoreFunction, null, beamWidth, 0.0f, ep, bits);
            var natural = toScratchCandidates(result.getNodes(), result.getNodes().length, naturalScratchPooled.get());
            updateNeighbors(graph.getNeighbors(node), natural, NodeArray.EMPTY);
        }
    }

    public void markNodeDeleted(int node) {
        graph.markDeleted(node);
    }

    /**
     * Remove nodes marked for deletion from the graph, and update neighbor lists
     * to maintain connectivity.
     *
     * @return approximate size of memory no longer used
     */
    private long removeDeletedNodes() {
        var deletedNodes = graph.getDeletedNodes();
        var nRemoved = deletedNodes.cardinality();
        if (nRemoved == 0) {
            return 0;
        }

        // remove the nodes from the graph, leaving holes and invalid neighbor references
        for (int i = deletedNodes.nextSetBit(0); i != NO_MORE_DOCS; i = deletedNodes.nextSetBit(i + 1)) {
            var success = graph.removeNode(i);
            assert success : String.format("Node %d marked deleted but not present", i);
        }
        var liveNodes = graph.rawNodes();

        // remove deleted nodes from neighbor lists.  If neighbor count drops below a minimum,
        // add random connections to preserve connectivity
        var affectedLiveNodes = new HashSet<Integer>();
        var R = new Random();
        try (var v1 = vectors.get();
             var v2 = vectorsCopy.get())
        {
            for (var node : liveNodes) {
                assert !deletedNodes.get(node);

                ConcurrentNeighborSet neighbors = graph.getNeighbors(node);
                if (neighbors.removeDeletedNeighbors(deletedNodes)) {
                    affectedLiveNodes.add(node);
                }

                // add random connections if we've dropped below minimum
                int minConnections = 1 + graph.maxDegree() / 2;
                if (neighbors.size() < minConnections) {
                    // create a NeighborArray of random connections
                    NodeArray randomConnections = new NodeArray(graph.maxDegree() - neighbors.size());
                    // doing actual sampling-without-replacement is expensive so we'll loop a fixed number of times instead
                    for (int i = 0; i < 2 * graph.maxDegree(); i++) {
                        int randomNode = liveNodes[R.nextInt(liveNodes.length)];
                        if (randomNode != node && !randomConnections.contains(randomNode)) {
                            float score = scoreBetween(v1.get().vectorValue(node), v2.get().vectorValue(randomNode));
                            randomConnections.insertSorted(randomNode, score);
                        }
                        if (randomConnections.size == randomConnections.node.length) {
                            break;
                        }
                    }
                    neighbors.padWithRandom(randomConnections);
                }
            }
        }

        // update entry node if old one was deleted
        if (deletedNodes.get(graph.entry())) {
            if (graph.size() > 0) {
                graph.updateEntryNode(graph.getNodes().nextInt());
            } else {
                graph.updateEntryNode(-1);
            }
        }

        // repair affected nodes
        for (var node : affectedLiveNodes) {
            addNNDescentConnections(node);
        }

        // reset deleted collection
        deletedNodes.clear();

        return nRemoved * graph.ramBytesUsedOneNode(0);
    }

    /**
     * Search for the given node, then submit all nodes along the search path as candidates for
     * new neighbors.  Standard diversity pruning applies.
     */
    private void addNNDescentConnections(int node) {
        var notSelfBits = createNotSelfBits(node);

        try (var gs = graphSearcher.get();
             var v1 = vectors.get();
             var v2 = vectorsCopy.get();
             var scratch = naturalScratch.get())
        {
            var value = v1.get().vectorValue(node);
            NodeSimilarity.ExactScoreFunction scoreFunction = i -> scoreBetween(v2.get().vectorValue(i), value);
            var result = gs.get().searchInternal(scoreFunction, null, beamWidth, 0.0f, graph.entry(), notSelfBits);
            var candidates = toScratchCandidates(result.getNodes(), result.getNodes().length, scratch.get());
            // We use just the topK results as candidates, which is much less expensive than computing scores for
            // the other visited nodes.  See comments in addGraphNode.
            updateNeighbors(graph.getNeighbors(node), candidates, NodeArray.EMPTY);
        }
    }

    private static Bits createNotSelfBits(int node) {
        return new Bits() {
            @Override
            public boolean get(int index) {
                return index != node;
            }

            @Override
            public int length() {
                // length is max node id, which could be larger than size after deletes
                throw new UnsupportedOperationException();
            }
        };
    }

    private int approximateMedioid() {
        assert graph.size() > 0;

        if (vectorEncoding != VectorEncoding.FLOAT32) {
            // fill this in when/if we care about byte[] vectors
            return graph.entry();
        }

        try (var gs = graphSearcher.get();
             var vc = vectorsCopy.get())
        {
            // compute centroid
            var centroid = new float[dimension];
            for (var it = graph.getNodes(); it.hasNext(); ) {
                var node = it.nextInt();
                VectorUtil.addInPlace(centroid, (float[]) vc.get().vectorValue(node));
            }
            VectorUtil.divInPlace(centroid, graph.size());

            // search for the node closest to the centroid
            NodeSimilarity.ExactScoreFunction scoreFunction = i -> scoreBetween(vc.get().vectorValue(i), (T) centroid);
            var result = gs.get().searchInternal(scoreFunction, null, beamWidth, 0.0f, graph.entry(), Bits.ALL);
            return result.getNodes()[0].node;
        }
    }

    private void updateNeighbors(ConcurrentNeighborSet neighbors, NodeArray natural, NodeArray concurrent) {
        neighbors.insertDiverse(natural, concurrent);
        neighbors.backlink(graph::getNeighbors, neighborOverflow);
    }

    private NodeArray toScratchCandidates(SearchResult.NodeScore[] candidates, int count, NodeArray scratch) {
        scratch.clear();
        for (int i = 0; i < count; i++) {
            var candidate = candidates[i];
            scratch.addInOrder(candidate.node, candidate.score);
        }
        return scratch;
    }

    private NodeArray getConcurrentCandidates(int newNode,
                                              Set<Integer> inProgress,
                                              NodeArray scratch,
                                              RandomAccessVectorValues<T> values,
                                              RandomAccessVectorValues<T> valuesCopy)
    {
        scratch.clear();
        for (var n : inProgress) {
            if (n != newNode) {
                scratch.insertSorted(
                        n,
                        scoreBetween(values.vectorValue(newNode), valuesCopy.vectorValue(n)));
            }
        }
        return scratch;
    }

    protected float scoreBetween(T v1, T v2) {
        return scoreBetween(vectorEncoding, similarityFunction, v1, v2);
    }

    static <T> float scoreBetween(
            VectorEncoding encoding, VectorSimilarityFunction similarityFunction, T v1, T v2) {
        switch (encoding) {
            case BYTE:
                return similarityFunction.compare((byte[]) v1, (byte[]) v2);
            case FLOAT32:
                return similarityFunction.compare((float[]) v1, (float[]) v2);
            default:
                throw new IllegalArgumentException();
        }
    }

    private static class ExcludingBits implements Bits {
        private final int excluded;

        public ExcludingBits(int excluded) {
            this.excluded = excluded;
        }

        @Override
        public boolean get(int index) {
            return index != excluded;
        }

        @Override
        public int length() {
            throw new UnsupportedOperationException();
        }
    }

    public void load(RandomAccessReader in) throws IOException {
        if (graph.size() != 0) {
            throw new IllegalStateException("Cannot load into a non-empty graph");
        }

        int size = in.readInt();
        int entryNode = in.readInt();
        int maxDegree = in.readInt();

        for (int i = 0; i < size; i++) {
            int node = in.readInt();
            int nNeighbors = in.readInt();
            var ca = new NodeArray(maxDegree);
            for (int j = 0; j < nNeighbors; j++) {
                int neighbor = in.readInt();
                ca.addInOrder(neighbor, similarity.score(node, neighbor));
            }
            graph.addNode(node, new ConcurrentNeighborSet(node, maxDegree, similarity, alpha, ca));
        }

        graph.updateEntryNode(entryNode);
    }
}
