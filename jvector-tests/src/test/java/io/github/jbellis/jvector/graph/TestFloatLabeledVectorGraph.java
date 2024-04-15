package io.github.jbellis.jvector.graph;

import io.github.jbellis.jvector.exceptions.ThreadInterruptedException;
import io.github.jbellis.jvector.graph.label.EntriesGraphView;
import io.github.jbellis.jvector.graph.label.LabelsChecker;
import io.github.jbellis.jvector.graph.label.LabelsSet;
import io.github.jbellis.jvector.graph.label.RandomAccessVectorLabels;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.util.GrowableBitSet;
import io.github.jbellis.jvector.vector.VectorEncoding;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.junit.Test;

import java.util.function.Function;

import static io.github.jbellis.jvector.graph.label.impl.BitLabelSet.asLabelSet;
import static org.junit.Assert.assertTrue;

public class TestFloatLabeledVectorGraph extends TestFloatVectorGraph {

    Function<Integer, RandomAccessVectorLabels<LabelsSet>> vectorLabels = (Integer size) -> new RandomAccessVectorLabels<>() {

        @Override
        public int size() {
            return size;
        }

        @Override
        public LabelsSet vectorLabels(int targetOrd) {
            return asLabelSet(1);
        }

        @Override
        public RandomAccessVectorLabels<LabelsSet> copy() {
            return vectorLabels.apply(size);
        }
    };
    @Override
    protected GraphIndexBuilderInterface<float[]> getBuilder(
            RandomAccessVectorValues<float[]> vectors,
            VectorEncoding vectorEncoding,
            VectorSimilarityFunction similarityFunction,
            int M,
            int beamWidth,
            float neighborOverflow,
            float alpha
    ) {
        return new LabeledGraphIndexBuilder<>(vectors, vectorLabels.apply(vectors.size()), vectorEncoding, similarityFunction, M, beamWidth, neighborOverflow, alpha);
    }

    @Override
    public OnHeapGraphIndexInterface<float[]> buildSequentially(GraphIndexBuilderInterface<float[]> builder, RandomAccessVectorValues<float[]> vectors) {
        for (var i = 0; i < vectors.size(); i++) {
            builder.addGraphNode(i, vectors, vectorLabels.apply(vectors.size()));
        }
        builder.cleanup();
        return builder.getGraph();
    }
    @Override
    protected long addGraphNode(GraphIndexBuilderInterface<float[]> builder, int node, RandomAccessVectorValues<float[]> vectors) {
        return builder.addGraphNode(node, vectors, vectorLabels.apply(vectors.size()));
    }

    @Override
    protected SearchResult search(
            float[] targetVector,
            int topK,
            RandomAccessVectorValues<float[]> vectors,
            VectorEncoding vectorEncoding,
            VectorSimilarityFunction similarityFunction,
            GraphIndex<float[]> graph,
            Bits acceptOrds) {
        var size = graph.getView().getIdUpperBound();
        LabeledGraphSearcher<float[]> searcher = new LabeledGraphSearcher<>((EntriesGraphView<float[]>)graph.getView(), new GrowableBitSet(size));
        LabelsChecker checker = n -> true;
        NodeSimilarity.ExactScoreFunction scoreFunction = i -> similarityFunction.compare(targetVector, vectors.vectorValue(i));
        return searcher.search(scoreFunction, checker, null, topK, acceptOrds);
    }

    @Override
    @Test
    public void testConcurrentNeighbors() {
        RandomAccessVectorValues<float[]> vectors = circularVectorValues(3);
        GraphIndexBuilderInterface<float[]> builder =
                new LabeledGraphIndexBuilder<>(vectors, vectorLabels.apply(vectors.size()), getVectorEncoding(), similarityFunction, 1, 30, 1.0f, 1.0f) {
                    @Override
                    protected float scoreBetween(float[] v1, float[] v2) {
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
}
