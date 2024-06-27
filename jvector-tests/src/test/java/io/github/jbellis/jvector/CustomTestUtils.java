package io.github.jbellis.jvector;

import io.github.jbellis.jvector.graph.GraphIndexBuilder;
import io.github.jbellis.jvector.graph.GraphSearcher;
import io.github.jbellis.jvector.graph.NodeSimilarity;
import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.graph.SearchResult;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.vector.VectorEncoding;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.apache.commons.math3.util.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class CustomTestUtils {

    public static float[] generateRandomVector(int dimension) {
        float[] vector = new float[dimension];
        for (int i = 0; i < dimension; i++) {
            vector[i] = ThreadLocalRandom.current().nextFloat();
            if (ThreadLocalRandom.current().nextBoolean()) {
                vector[i] = -vector[i];
            }
        }
        return vector;
    }

    public static void addNode(MutableListVectorValues vectorsValue, GraphIndexBuilder<float[]> builder, int id, float[] vector) {
        vectorsValue.put(id, vector);
        builder.addGraphNode(id, vectorsValue);
    }

    public static GraphIndexBuilder<float[]> graphBuilder(
            RandomAccessVectorValues<float[]> vectorsValue,
            VectorSimilarityFunction metric,
            int m,
            int ef
    ) {

        return new GraphIndexBuilder<>(
                vectorsValue,
                VectorEncoding.FLOAT32,
                metric,
                m, // number of neighbors = M * 2
                ef,
                1.0f,
                1.5f
        );
    }

    public static List<Pair<Integer, Float>> search(
            float[] vector,
            int topK,
            GraphSearcher<float[]> searcher,
            RandomAccessVectorValues<float[]> vectorsValue,
            VectorSimilarityFunction function
    ) {

        NodeSimilarity.ExactScoreFunction scoreFunction = (j) -> function.compare(vector, vectorsValue.vectorValue(j));

        Bits mask = new Bits.MatchAllBits();
        SearchResult searchResult = searcher.search(scoreFunction, null, topK, mask);
        List<Pair<Integer, Float>> resultsList = new ArrayList<>();
        for (SearchResult.NodeScore nodeScore : searchResult.getNodes()) {
            resultsList.add(new Pair<>(nodeScore.node, nodeScore.score));
        }
        return resultsList;
    }

    public static List<Pair<Integer, Float>> searchEuclidean(
            float[] vector,
            int topK,
            GraphIndexBuilder<float[]> builder,
            RandomAccessVectorValues<float[]> vectorsValue
    ) {
        GraphSearcher<float[]> searcher = new GraphSearcher.Builder<>(builder.getGraph().getView()).withConcurrentUpdates().build();
        return search(vector, topK, searcher, vectorsValue, VectorSimilarityFunction.EUCLIDEAN);
    }

    public static class MutableListVectorValues implements RandomAccessVectorValues<float[]> {

        Map<Integer, float[]> vectors = new HashMap<>();

        Integer maxOrd = Integer.MIN_VALUE;

        public MutableListVectorValues(int dim) {
            this.dim = dim;

        }

        public MutableListVectorValues(MutableListVectorValues list) {
            this.dim = list.dim;
            this.vectors = list.vectors;
            this.maxOrd = list.maxOrd;
        }

        int dim;

        @Override
        public int size() {
            return vectors.size();
        }

        @Override
        public int dimension() {
            return dim;
        }

        @Override
        public float[] vectorValue(int targetOrd) {
            return vectors.get(targetOrd);
        }

        public void put(int targetOrd, float[] vector) {
            vectors.put(targetOrd, vector);
            maxOrd = Math.max(targetOrd, maxOrd);
        }

        public void delete(int targetOrd) {
            vectors.remove(targetOrd);
        }

        @Override
        public boolean isValueShared() {
            return false;
        }

        @Override
        public RandomAccessVectorValues<float[]> copy() {
            return new MutableListVectorValues(this);
        }
    }

}
