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

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import io.github.jbellis.jvector.LuceneTestCase;
import io.github.jbellis.jvector.TestUtil;
import io.github.jbellis.jvector.exceptions.ThreadInterruptedException;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.util.BoundedLongHeap;
import io.github.jbellis.jvector.util.FixedBitSet;
import io.github.jbellis.jvector.vector.VectorEncoding;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests KNN graphs
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public abstract class GraphIndexTestCase<T> extends LuceneTestCase {

    VectorSimilarityFunction similarityFunction;

    abstract VectorEncoding getVectorEncoding();

    abstract T randomVector(int dim);

    abstract AbstractMockVectorValues<T> vectorValues(int size, int dimension);

    abstract AbstractMockVectorValues<T> vectorValues(float[][] values);

    abstract RandomAccessVectorValues<T> circularVectorValues(int nDoc);

    abstract T getTargetVector();

    protected GraphIndexBuilderInterface<T> getBuilder(
            RandomAccessVectorValues<T> vectors,
            VectorEncoding vectorEncoding,
            VectorSimilarityFunction similarityFunction,
            int M,
            int beamWidth,
            float neighborOverflow,
            float alpha
    ) {
        return new GraphIndexBuilder<>(vectors, vectorEncoding, similarityFunction, M, beamWidth, neighborOverflow, alpha);
    }

    protected OnHeapGraphIndexInterface<T> buildSequentially(GraphIndexBuilderInterface<T> builder, RandomAccessVectorValues<T> vectors) {
        return TestUtil.buildSequentially(builder, vectors);
    }

    protected long addGraphNode(GraphIndexBuilderInterface<T> builder, int node, RandomAccessVectorValues<T> vectors) {
        return builder.addGraphNode(node, vectors);
    }

    protected SearchResult search(T targetVector, int topK, RandomAccessVectorValues<T> vectors, VectorEncoding vectorEncoding, VectorSimilarityFunction similarityFunction, GraphIndex<T> graph, Bits acceptOrds) {
        return GraphSearcher.search(targetVector, topK, vectors, vectorEncoding, similarityFunction, graph, acceptOrds);
    }



    // Make sure we actually approximately find the closest k elements. Mostly this is about
    // ensuring that we have all the distance functions, comparators, priority queues and so on
    // oriented in the right directions
    @Test
    public void testAknnDiverse() {
        int nDoc = 100;
        similarityFunction = VectorSimilarityFunction.DOT_PRODUCT;
        RandomAccessVectorValues<T> vectors = circularVectorValues(nDoc);
        VectorEncoding vectorEncoding = getVectorEncoding();
        GraphIndexBuilderInterface<T> builder = getBuilder(vectors, vectorEncoding, similarityFunction, 10, 100, 1.0f, 1.4f);
        var graph = buildSequentially(builder, vectors);
        // run some searches
        SearchResult.NodeScore[] nn = search(getTargetVector(),
                                                           10,
                                                           vectors.copy(),
                                                           getVectorEncoding(),
                                                           similarityFunction,
                                                           graph,
                                                           Bits.ALL
        ).getNodes();
        int[] nodes = Arrays.stream(nn).mapToInt(nodeScore -> nodeScore.node).toArray();
        assertEquals("Number of found results is not equal to [10].", 10, nodes.length);
        int sum = 0;
        for (int node : nodes) {
            sum += node;
        }
        // We expect to get approximately 100% recall;
        // the lowest docIds are closest to zero; sum(0,9) = 45
        assertTrue("sum(result docs)=" + sum + " for " + GraphIndex.prettyPrint(builder.getGraph()), sum < 75);

        for (int i = 0; i < nDoc; i++) {
            ConcurrentNeighborSet neighbors = graph.getNeighbors(i);
            Iterator<Integer> it = neighbors.iterator();
            while (it.hasNext()) {
                // all neighbors should be valid node ids.
                assertTrue(it.next() < nDoc);
            }
        }
    }

    @Test
    public void testSearchWithAcceptOrds() {
        int nDoc = 100;
        RandomAccessVectorValues<T> vectors = circularVectorValues(nDoc);
        similarityFunction = VectorSimilarityFunction.DOT_PRODUCT;
        VectorEncoding vectorEncoding = getVectorEncoding();
        GraphIndexBuilderInterface<T> builder =
                getBuilder(vectors, vectorEncoding, similarityFunction, 16, 100, 1.0f, 1.4f);
        var graph = buildSequentially(builder, vectors);
        // the first 10 docs must not be deleted to ensure the expected recall
        Bits acceptOrds = createRandomAcceptOrds(10, nDoc);
        SearchResult.NodeScore[] nn = search(getTargetVector(),
                                                           10,
                                                           vectors.copy(),
                                                           getVectorEncoding(),
                                                           similarityFunction,
                                                           graph,
                                                           acceptOrds
        ).getNodes();
        int[] nodes = Arrays.stream(nn).mapToInt(nodeScore -> nodeScore.node).toArray();
        assertEquals("Number of found results is not equal to [10].", 10, nodes.length);
        int sum = 0;
        for (int node : nodes) {
            assertTrue("the results include a deleted document: " + node, acceptOrds.get(node));
            sum += node;
        }
        // We expect to get approximately 100% recall;
        // the lowest docIds are closest to zero; sum(0,9) = 45
        assertTrue("sum(result docs)=" + sum + " for " + GraphIndex.prettyPrint(builder.getGraph()), sum < 75);
    }

    @Test
    public void testSearchWithSelectiveAcceptOrds() {
        int nDoc = 100;
        RandomAccessVectorValues<T> vectors = circularVectorValues(nDoc);
        similarityFunction = VectorSimilarityFunction.DOT_PRODUCT;
        VectorEncoding vectorEncoding = getVectorEncoding();
        GraphIndexBuilderInterface<T> builder =
                getBuilder(vectors, vectorEncoding, similarityFunction, 16, 100, 1.0f, 1.4f);
        var graph = buildSequentially(builder, vectors);
        // Only mark a few vectors as accepted
        var acceptOrds = new FixedBitSet(nDoc);
        for (int i = 0; i < nDoc; i += nextInt(15, 20)) {
            acceptOrds.set(i);
        }

        // Check the search finds all accepted vectors
        int numAccepted = acceptOrds.cardinality();
        SearchResult.NodeScore[] nn = search(getTargetVector(),
                                                           numAccepted,
                                                           vectors.copy(),
                                                           getVectorEncoding(),
                                                           similarityFunction,
                                                           graph,
                                                           acceptOrds
        ).getNodes();

        int[] nodes = Arrays.stream(nn).mapToInt(nodeScore -> nodeScore.node).toArray();
        for (int node : nodes) {
            assertTrue(String.format("the results include a deleted document: %d for %s",
                                     node, GraphIndex.prettyPrint(builder.getGraph())), acceptOrds.get(node));
        }
        for (int i = 0; i < acceptOrds.length(); i++) {
            if (acceptOrds.get(i)) {
                int finalI = i;
                assertTrue(String.format("the results do not include an accepted document: %d for %s",
                                         i, GraphIndex.prettyPrint(builder.getGraph())), Arrays.stream(nodes).anyMatch(j -> j == finalI));
            }
        }
    }

    @Test
    public void testGraphIndexBuilderInvalid() {
        assertThrows(NullPointerException.class,
                     () -> getBuilder(null, null, null, 0, 0, 1.0f, 1.0f));
        // M must be > 0
        assertThrows(IllegalArgumentException.class,
                     () -> {
                         RandomAccessVectorValues<T> vectors = vectorValues(1, 1);
                         VectorEncoding vectorEncoding = getVectorEncoding();
                         getBuilder(vectors, vectorEncoding, similarityFunction, 0, 10, 1.0f, 1.0f);
                     });
        // beamWidth must be > 0
        assertThrows(IllegalArgumentException.class,
                     () -> {
                         RandomAccessVectorValues<T> vectors = vectorValues(1, 1);
                         VectorEncoding vectorEncoding = getVectorEncoding();
                         getBuilder(vectors, vectorEncoding, similarityFunction, 10, 0, 1.0f, 1.0f);
                     });
    }

    // FIXME
    @Test
    public void testRamUsageEstimate() {
    }

    @Test
    public void testDiversity() {
        similarityFunction = VectorSimilarityFunction.DOT_PRODUCT;
        // Some carefully checked test cases with simple 2d vectors on the unit circle:
        float[][] values = {
                unitVector2d(0.5),
                unitVector2d(0.75),
                unitVector2d(0.2),
                unitVector2d(0.9),
                unitVector2d(0.8),
                unitVector2d(0.77),
                unitVector2d(0.6)
        };
        AbstractMockVectorValues<T> vectors = vectorValues(values);
        // First add nodes until everybody gets a full neighbor list
        VectorEncoding vectorEncoding = getVectorEncoding();
        GraphIndexBuilderInterface<T> builder =
                getBuilder(vectors, vectorEncoding, similarityFunction, 2, 10, 1.0f, 1.0f);
        // node 0 is added by the builder constructor
        addGraphNode(builder, 0, vectors);
        addGraphNode(builder, 1, vectors);
        addGraphNode(builder, 2, vectors);
        // now every node has tried to attach every other node as a neighbor, but
        // some were excluded based on diversity check.
        assertLevel0Neighbors(builder.getGraph(), 0, 1, 2);
        assertLevel0Neighbors(builder.getGraph(), 1, 0);
        assertLevel0Neighbors(builder.getGraph(), 2, 0);

        addGraphNode(builder, 3, vectors);
        assertLevel0Neighbors(builder.getGraph(), 0, 1, 2);
        // we added 3 here
        assertLevel0Neighbors(builder.getGraph(), 1, 0, 3);
        assertLevel0Neighbors(builder.getGraph(), 2, 0);
        assertLevel0Neighbors(builder.getGraph(), 3, 1);

        // supplant an existing neighbor
        addGraphNode(builder, 4, vectors);
        // 4 is the same distance from 0 that 2 is; we leave the existing node in place
        assertLevel0Neighbors(builder.getGraph(), 0, 1, 2);
        assertLevel0Neighbors(builder.getGraph(), 1, 0, 3, 4);
        assertLevel0Neighbors(builder.getGraph(), 2, 0);
        // 1 survives the diversity check
        assertLevel0Neighbors(builder.getGraph(), 3, 1, 4);
        assertLevel0Neighbors(builder.getGraph(), 4, 1, 3);

        addGraphNode(builder,5, vectors);
        assertLevel0Neighbors(builder.getGraph(), 0, 1, 2);
        assertLevel0Neighbors(builder.getGraph(), 1, 0, 3, 4, 5);
        assertLevel0Neighbors(builder.getGraph(), 2, 0);
        // even though 5 is closer, 3 is not a neighbor of 5, so no update to *its* neighbors occurs
        assertLevel0Neighbors(builder.getGraph(), 3, 1, 4);
        assertLevel0Neighbors(builder.getGraph(), 4, 1, 3, 5);
        assertLevel0Neighbors(builder.getGraph(), 5, 1, 4);
    }

    @Test
    public void testDiversityFallback() {
        similarityFunction = VectorSimilarityFunction.EUCLIDEAN;
        // Some test cases can't be exercised in two dimensions;
        // in particular if a new neighbor displaces an existing neighbor
        // by being closer to the target, yet none of the existing neighbors is closer to the new vector
        // than to the target -- ie they all remain diverse, so we simply drop the farthest one.
        float[][] values = {
                {0, 0, 0},
                {0, 10, 0},
                {0, 0, 20},
                {10, 0, 0},
                {0, 4, 0}
        };
        AbstractMockVectorValues<T> vectors = vectorValues(values);
        // First add nodes until everybody gets a full neighbor list
        VectorEncoding vectorEncoding = getVectorEncoding();
        GraphIndexBuilderInterface<T> builder =
                getBuilder(vectors, vectorEncoding, similarityFunction, 1, 10, 1.0f, 1.0f);
        addGraphNode(builder,0, vectors);
        addGraphNode(builder,1, vectors);
        addGraphNode(builder,2, vectors);
        assertLevel0Neighbors(builder.getGraph(), 0, 1, 2);
        // 2 is closer to 0 than 1, so it is excluded as non-diverse
        assertLevel0Neighbors(builder.getGraph(), 1, 0);
        // 1 is closer to 0 than 2, so it is excluded as non-diverse
        assertLevel0Neighbors(builder.getGraph(), 2, 0);

        addGraphNode(builder,3, vectors);
        // this is one case we are testing; 2 has been displaced by 3
        assertLevel0Neighbors(builder.getGraph(), 0, 1, 3);
        assertLevel0Neighbors(builder.getGraph(), 1, 0);
        assertLevel0Neighbors(builder.getGraph(), 2, 0);
        assertLevel0Neighbors(builder.getGraph(), 3, 0);
    }

    @Test
    public void testDiversity3d() {
        similarityFunction = VectorSimilarityFunction.EUCLIDEAN;
        // test the case when a neighbor *becomes* non-diverse when a newer better neighbor arrives
        float[][] values = {
                {0, 0, 0},
                {0, 10, 0},
                {0, 0, 20},
                {0, 9, 0}
        };
        AbstractMockVectorValues<T> vectors = vectorValues(values);
        // First add nodes until everybody gets a full neighbor list
        VectorEncoding vectorEncoding = getVectorEncoding();
        GraphIndexBuilderInterface<T> builder =
                getBuilder(vectors, vectorEncoding, similarityFunction, 1, 10, 1.0f, 1.0f);
        addGraphNode(builder,0, vectors);
        addGraphNode(builder,1, vectors);
        addGraphNode(builder,2, vectors);
        assertLevel0Neighbors(builder.getGraph(), 0, 1, 2);
        // 2 is closer to 0 than 1, so it is excluded as non-diverse
        assertLevel0Neighbors(builder.getGraph(), 1, 0);
        // 1 is closer to 0 than 2, so it is excluded as non-diverse
        assertLevel0Neighbors(builder.getGraph(), 2, 0);

        addGraphNode(builder,3, vectors);
        // this is one case we are testing; 1 has been displaced by 3
        assertLevel0Neighbors(builder.getGraph(), 0, 2, 3);
        assertLevel0Neighbors(builder.getGraph(), 1, 0, 3);
        assertLevel0Neighbors(builder.getGraph(), 2, 0);
        assertLevel0Neighbors(builder.getGraph(), 3, 0, 1);
    }

    private void assertLevel0Neighbors(OnHeapGraphIndexInterface<T> graph, int node, int... expected) {
        Arrays.sort(expected);
        ConcurrentNeighborSet nn = graph.getNeighbors(node);
        Iterator<Integer> it = nn.iterator();
        int[] actual = new int[nn.size()];
        for (int i = 0; i < actual.length; i++) {
            actual[i] = it.next();
        }
        Arrays.sort(actual);
        assertArrayEquals(expected, actual);
    }

    @Test
    // build a random graph, then check that it has at least 90% recall
    public void testRandom() {
        int size = between(100, 150);
        int dim = between(2, 15);
        AbstractMockVectorValues<T> vectors = vectorValues(size, dim);
        int topK = 5;
        VectorEncoding vectorEncoding = getVectorEncoding();
        GraphIndexBuilderInterface<T> builder =
                getBuilder(vectors, vectorEncoding, similarityFunction, 10, 30, 1.0f, 1.4f);
        var graph = builder.build();
        Bits acceptOrds = getRandom().nextBoolean() ? Bits.ALL : createRandomAcceptOrds(0, size);

        int efSearch = 100;
        int totalMatches = 0;
        for (int i = 0; i < 100; i++) {
            SearchResult.NodeScore[] actual;
            T query = randomVector(dim);
            actual = search(query,
                                          efSearch,
                                          vectors,
                                          getVectorEncoding(),
                                          similarityFunction,
                                          graph,
                                          acceptOrds
            ).getNodes();

            NodeQueue expected = new NodeQueue(new BoundedLongHeap(topK), NodeQueue.Order.MIN_HEAP);
            for (int j = 0; j < size; j++) {
                if (vectors.vectorValue(j) != null && acceptOrds.get(j)) {
                    if (getVectorEncoding() == VectorEncoding.BYTE) {
                        assert query instanceof byte[];
                        expected.push(j, similarityFunction.compare((byte[]) query, (byte[]) vectors.vectorValue(j)));
                    } else {
                        assert query instanceof float[];
                        expected.push(j, similarityFunction.compare((float[]) query, (float[]) vectors.vectorValue(j)));
                    }
                }
            }
            var actualNodeIds = Arrays.stream(actual, 0, topK).mapToInt(nodeScore -> nodeScore.node).toArray();

            assertEquals(topK, actualNodeIds.length);
            totalMatches += computeOverlap(actualNodeIds, expected.nodesCopy());
        }
        // with the current settings, we can visit every node in the graph, so this should actually be 100%
        // except in cases where the graph ends up partitioned.  If that happens, it probably means
        // a bug has been introduced in graph construction.
        double overlap = totalMatches / (double) (100 * topK);
        assertTrue("overlap=" + overlap, overlap > 0.9);
    }

    private int computeOverlap(int[] a, int[] b) {
        Arrays.sort(a);
        Arrays.sort(b);
        int overlap = 0;
        for (int i = 0, j = 0; i < a.length && j < b.length; ) {
            if (a[i] == b[j]) {
                ++overlap;
                ++i;
                ++j;
            } else if (a[i] > b[j]) {
                ++j;
            } else {
                ++i;
            }
        }
        return overlap;
    }

    @Test
    public void testConcurrentNeighbors() {
        RandomAccessVectorValues<T> vectors = circularVectorValues(3);
        GraphIndexBuilderInterface<T> builder =
                new GraphIndexBuilder<>(vectors, getVectorEncoding(), similarityFunction, 1, 30, 1.0f, 1.0f) {
                    @Override
                    protected float scoreBetween(T v1, T v2) {
                        try {
                            Thread.sleep(10);
                        } catch (InterruptedException e) {
                            throw new ThreadInterruptedException(e);
                        }
                        return super.scoreBetween(v1, v2);
                    }
                };
        var graph = builder.build();
        for (int i = 0; i < vectors.size(); i++) {
            assertTrue(graph.getNeighbors(i).size() <= 2); // Level 0 gets 2x neighbors
        }
    }

    /**
     * Returns vectors evenly distributed around the upper unit semicircle.
     */
    public static class CircularFloatVectorValues implements RandomAccessVectorValues<float[]> {

        private final int size;

        public CircularFloatVectorValues(int size) {
            this.size = size;
        }

        @Override
        public CircularFloatVectorValues copy() {
            return new CircularFloatVectorValues(size);
        }

        @Override
        public int dimension() {
            return 2;
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public float[] vectorValue(int ord) {
            return unitVector2d(ord / (double) size);
        }

        @Override
        public boolean isValueShared() {
            return false;
        }
    }

    /**
     * Returns vectors evenly distributed around the upper unit semicircle.
     */
    static class CircularByteVectorValues implements RandomAccessVectorValues<byte[]> {
        private final int size;

        CircularByteVectorValues(int size) {
            this.size = size;
        }

        @Override
        public CircularByteVectorValues copy() {
            return new CircularByteVectorValues(size);
        }

        @Override
        public int dimension() {
            return 2;
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public byte[] vectorValue(int ord) {
            float[] value = unitVector2d(ord / (double) size);
            byte[] bValue = new byte[value.length];
            for (int i = 0; i < value.length; i++) {
                bValue[i] = (byte) (value[i] * 127);
            }
            return bValue;
        }

        @Override
        public boolean isValueShared() {
            return false;
        }
    }

    private static float[] unitVector2d(double piRadians) {
        return new float[] {
                (float) Math.cos(Math.PI * piRadians), (float) Math.sin(Math.PI * piRadians)
        };
    }

    public static float[][] createRandomFloatVectors(int size, int dimension, Random random) {
        float[][] vectors = new float[size][];
        for (int offset = 0; offset < size; offset++) {
            vectors[offset] = TestUtil.randomVector(random, dimension);
        }
        return vectors;
    }

    static byte[][] createRandomByteVectors(int size, int dimension, Random random) {
        byte[][] vectors = new byte[size][];
        for (int offset = 0; offset < size; offset++) {
            vectors[offset] = TestUtil.randomVector8(random, dimension);
        }
        return vectors;
    }

    /**
     * Generate a random bitset where before startIndex all bits are set, and after startIndex each
     * entry has a 2/3 probability of being set.
     */
    private static Bits createRandomAcceptOrds(int startIndex, int length) {
        FixedBitSet bits = new FixedBitSet(length);
        // all bits are set before startIndex
        for (int i = 0; i < startIndex; i++) {
            bits.set(i);
        }
        // after startIndex, bits are set with 2/3 probability
        for (int i = startIndex; i < bits.length(); i++) {
            if (getRandom().nextFloat() < 0.667f) {
                bits.set(i);
            }
        }
        return bits;
    }
}
