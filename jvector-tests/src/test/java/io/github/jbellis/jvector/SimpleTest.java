package io.github.jbellis.jvector;

import io.github.jbellis.jvector.graph.GraphIndex;
import io.github.jbellis.jvector.graph.GraphIndexBuilder;
import io.github.jbellis.jvector.graph.GraphSearcher;
import io.github.jbellis.jvector.graph.ListRandomAccessVectorValues;
import io.github.jbellis.jvector.graph.NodeSimilarity;
import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.graph.SearchResult;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.util.FixedBitSet;
import io.github.jbellis.jvector.util.SparseFixedBitSet;
import io.github.jbellis.jvector.vector.VectorEncoding;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.apache.commons.math3.util.Pair;
import org.junit.Test;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static java.util.stream.IntStream.range;
import static org.junit.Assert.assertTrue;

public class SimpleTest {

    @Test
    public void dedup() {
        List<float[]> vectors = new ArrayList<>();
        float[][] blocks = new float[][]{new float[]{0f, 0f}, new float[]{10f, 0f}, new float[]{0f, 10f}, new float[]{10f, 10f}};
        int numInBlock = 3;
//        range(0, blocks.length).forEach(block -> range(0, numInBlock).forEach(
//                element -> vectors.add(blocks[block])
//        ));

        range(0, numInBlock).forEach(el -> range(0, blocks.length).forEach(
                block -> vectors.add(blocks[block])
        ));
        var vectorsValue1 = new ListRandomAccessVectorValues(vectors, 2);
        var graph1 = buildGraph(vectorsValue1);
        graph1.getNodes().forEachRemaining((int i) -> System.out.println(i + ": " + vectorsValue1.vectorValue(i)[0] + ", " + vectorsValue1.vectorValue(i)[1]));

        var representation = graph1.getGraphRepresentation();
        System.out.println(representation);
        var searchVector = new float[]{1, 1};

        var result1 = searchEuclidean(searchVector, 2, graph1.getView(), vectorsValue1, Collections.emptySet());
        System.out.println("***********");
        System.out.println(result1);
    }

    @Test
    public void dedupByOne() {
        var vectorsValue = new MutableListVectorValues(2);

        var builder = graphBuilder(vectorsValue, VectorSimilarityFunction.EUCLIDEAN);
        var graph = builder.getGraph();
        range(0, 3).forEach(i -> addNode(vectorsValue, builder, i, new float[]{i, 0}));
        addNode(vectorsValue, builder, 11, new float[]{0, 0});
        addNode(vectorsValue, builder, 12, new float[]{0, 0});
        //   addNode(vectorsValue, builder, 12, new float[]{0, 0});
        //   addNode(vectorsValue, builder, 13, new float[]{0, 0});
//        addNode(vectorsValue, builder, 3, new float[]{2, 0});
//        addNode(vectorsValue, builder, 4, new float[]{3, 0});
//        addNode(vectorsValue, builder, 5, new float[]{4, 0});
        // addNode(vectorsValue, builder, 4, new float[]{0, 0});
        // addNode(vectorsValue, builder, 5, new float[]{0, 0});

        var graphRepresentation = graph.getGraphRepresentation();
        System.out.println("final:" + graphRepresentation);

        var searchVector = new float[]{1, 1};

        var result1 = searchEuclidean(searchVector, 1, graph.getView(), vectorsValue, Collections.emptySet());
        System.out.println("***********");
        System.out.println(result1);

    }

    @Test
    public void BrokenGraph_sameCoordinates() {
        var vectorsValue = new MutableListVectorValues(2);

        var builder = graphBuilder(vectorsValue, VectorSimilarityFunction.EUCLIDEAN);
        var graph = builder.getGraph();
        addNode(vectorsValue, builder, 0, new float[]{0, 0});

        addNode(vectorsValue, builder, 1, new float[]{1, 1});
        addNode(vectorsValue, builder, 2, new float[]{2, 3});
        addNode(vectorsValue, builder, 3, new float[]{5, 4});

        //     range(0, 10).forEach(i -> addNode(vectorsValue, builder, 10 + i, new float[]{1 + r.nextInt(10), 1 + r.nextInt(10)}));

        addNode(vectorsValue, builder, 4, new float[]{0, 0});
        addNode(vectorsValue, builder, 5, new float[]{0, 0});

        var graphRepresentation = graph.getGraphRepresentation();
        System.out.println("final:" + graphRepresentation);
        System.out.println("start:" + graph.startNode());

        var searchVector = new float[]{2, 0};

        var result1 = searchEuclidean(searchVector, 1, graph.getView(), vectorsValue, Collections.emptySet());
        System.out.println("***********");
        System.out.println(result1);

    }

    @Test
    public void BrokenGraph_sameCoordinates_moreNeighbours() {
        var entry = new float[]{50, 50};
        var vectorsValue = new MutableListVectorValues(2);
        Random r = new Random();
        Supplier<float[]> vectorGenerator = () -> {
            while (true) {
                var vector = new float[]{
                        r.nextInt(100),
                        r.nextInt(100)
                };
                if (!Arrays.equals(entry, vector)) {
                    return vector;
                }
            }
        };

        int maxConnections = 5;

        GraphIndexBuilder builder = new GraphIndexBuilder(
                vectorsValue,
                VectorEncoding.FLOAT32,
                VectorSimilarityFunction.EUCLIDEAN,
                5, // number of neighbors = M * 2
                10,
                0.0f,
                1.5f
        );
        var graph = builder.getGraph();
        AtomicInteger setId = new AtomicInteger();

        range(1, 2 * maxConnections + 2).forEach(b -> {
            addNode(vectorsValue, builder, setId.get(), entry);
            range(0, 10).forEach(i -> addNode(vectorsValue, builder, b * 100 + i, vectorGenerator.get()));
            setId.getAndIncrement();
        });

        var graphRepresentation = graph.getGraphRepresentation();
        System.out.println("final:" + graphRepresentation);
        System.out.println("start:" + graph.startNode());

        for(int i = -100; i < 200; i++) {
            for(int j = -100; j < 200; j++) {
                var searchVector = new float[]{0, 0};
                List<Pair<Integer, Float>> results = searchEuclidean(searchVector, 3, graph.getView(), vectorsValue, Collections.emptySet());
                for(var res: results) {
                    assertTrue(res.getFirst() < 100);
                }
            }
        }
        writeToFile("broken", vectorsValue, graphRepresentation);
    }

    @Test
    public void BrokenGraph_closePointsWithDot() {
        Random r = new Random();
        Supplier<float[]> generateVector = () -> new float[]{
                r.nextInt(100),
                r.nextInt(100),
                r.nextInt(100)
        };

        float[][] vectorsWithSameDot = new float[][]{new float[]{200, 200, 0}, new float[]{200, 0, 200}, new float[]{0, 200, 200}};

        // check dot
        for (float[] v1 : vectorsWithSameDot) {
            for (float[] v2 : vectorsWithSameDot) {
                if (v1 == v2) {
                    continue;
                }
                //   assertEquals(2.5, VectorSimilarityFunction.DOT_PRODUCT.compare(v1, v2), 0.001);
            }
        }
        var vectorsValue = new MutableListVectorValues(3);

        var builder = graphBuilder(vectorsValue, VectorSimilarityFunction.DOT_PRODUCT);
        var graph = builder.getGraph();
        addNode(vectorsValue, builder, 0, vectorsWithSameDot[0]);

        range(0, 2).forEach(i -> addNode(vectorsValue, builder, 100 + i, generateVector.get()));

        addNode(vectorsValue, builder, 4, vectorsWithSameDot[1]);

        range(0, 2).forEach(i -> addNode(vectorsValue, builder, 200 + i, generateVector.get()));

        addNode(vectorsValue, builder, 5, vectorsWithSameDot[2]);

        range(0, 2).forEach(i -> addNode(vectorsValue, builder, 300 + i, generateVector.get()));

        var graphRepresentation = graph.getGraphRepresentation();
        System.out.println("final:" + graphRepresentation);
        System.out.println("start:" + graph.startNode());
        vectorsValue.vectors.forEach((node, value) -> {
            //       System.out.println("node:" + node + " value: [" + value[0] + ", " + value[1] + "]");
        });

        range(0, 100).forEach(
                i -> range(0, 100).forEach(
                        j -> range(0, 100).forEach(
                                k -> {
                                    var searchVector = new float[]{i, j, k};
                                    List<Pair<Integer, Float>> result = search(
                                            searchVector,
                                            1,
                                            graph.getView(),
                                            vectorsValue,
                                            Collections.emptySet(),
                                            VectorSimilarityFunction.DOT_PRODUCT
                                    );
                                    int node = result.get(0).getFirst();
                                    assertTrue(Set.of(0, 4, 5).contains(node));
                                }
                        )
                )
        );
        var searchVector = new float[]{200, 0, 200};

        var result1 = search(searchVector, 5, graph.getView(), vectorsValue, Collections.emptySet(), VectorSimilarityFunction.DOT_PRODUCT);
        System.out.println("***********");
        System.out.println(result1);


    }

    @Test
    public void BrokenGraph_withCosine() {
        Random r = new Random();
        Supplier<float[]> generateVector = () -> new float[]{r.nextInt(50), 51 + r.nextInt(50)};
        var vectorsValue = new MutableListVectorValues(2);

        var builder = graphBuilder(vectorsValue, VectorSimilarityFunction.COSINE);
        var graph = builder.getGraph();
        addNode(vectorsValue, builder, 0, new float[]{1, 1});

        range(0, 30).forEach(i -> addNode(vectorsValue, builder, 100 + i, generateVector.get()));

        addNode(vectorsValue, builder, 4, new float[]{2, 2});

        range(0, 30).forEach(i -> addNode(vectorsValue, builder, 200 + i, generateVector.get()));

        addNode(vectorsValue, builder, 5, new float[]{3, 3});

        range(0, 30).forEach(i -> addNode(vectorsValue, builder, 300 + i, generateVector.get()));

        var graphRepresentation = graph.getGraphRepresentation();
        System.out.println("final:" + graphRepresentation);
        System.out.println("start:" + graph.startNode());
        vectorsValue.vectors.forEach((node, value) -> {
            //       System.out.println("node:" + node + " value: [" + value[0] + ", " + value[1] + "]");
        });

        range(1, 100).forEach(i -> range(1, 100).forEach(j -> {
            var searchVector = new float[]{i, j};
            List<Pair<Integer, Float>> result = search(searchVector, 1, graph.getView(), vectorsValue, Collections.emptySet(), VectorSimilarityFunction.COSINE);
            int node = result.get(0).getFirst();
            assertTrue(Set.of(0, 4, 5).contains(node));

        }));
        var searchVector = new float[]{1, 1};

        var result1 = search(searchVector, 5, graph.getView(), vectorsValue, Collections.emptySet(), VectorSimilarityFunction.COSINE);
        System.out.println("***********");
        System.out.println(result1);

        var t1 = VectorSimilarityFunction.COSINE.compare(new float[]{1, 1}, new float[]{2, 2});
        var t2 = VectorSimilarityFunction.COSINE.compare(new float[]{1, 1}, new float[]{1, 1});
        System.out.println(t1);
        System.out.println(t2);
    }

    @Test
    public void generateRandomGraph() throws IOException {
        int size = 1000;
        Random r = new Random();
        Supplier<float[]> vectorGenerator = () -> new float[]{r.nextInt(1000), r.nextInt(1000)};
        var vectorsValue = new MutableListVectorValues(2);

        GraphIndexBuilder builder = new GraphIndexBuilder(
                vectorsValue,
                VectorEncoding.FLOAT32,
                VectorSimilarityFunction.EUCLIDEAN,
                10, // number of neighbors = M * 2
                20,
                0.0f,
                1.5f
        );

        range(0, size).forEach(i -> addNode(vectorsValue, builder, i, vectorGenerator.get()));

        var graph = builder.getGraph();
        Map<Integer, List<Integer>> graphRepresentation = graph.getGraphRepresentation();
        System.out.println("final:" + graphRepresentation);
        System.out.println("start:" + graph.startNode());

        writeToFile("random", vectorsValue, graphRepresentation);
    }


    private void writeToFile(String prefix, MutableListVectorValues vectorsValue, Map<Integer, List<Integer>> graphRepresentation) {
        PrintWriter vectorWriter = null;
        PrintWriter edgesWriter = null;
        try {
            vectorWriter = new PrintWriter(new FileWriter(prefix + "_vectors.csv"));
            edgesWriter = new PrintWriter(new FileWriter(prefix + "_edges.csv"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        for (int i = 0; i <= vectorsValue.maxOrd; i++) {
            var vector = vectorsValue.vectors.getOrDefault(i, null);
            if(vector == null) {
                continue;
            }
            vectorWriter.println(i + ", " + vector[0] + ", " + vector[1]);
            var nbrs = graphRepresentation.get(i);
            for (var nbr : nbrs) {
                edgesWriter.println(i + ", " + nbr);
            }
        }
        vectorWriter.close();
        edgesWriter.close();
    }

    private void addNode(MutableListVectorValues vectorsValue, GraphIndexBuilder builder, int id, float[] vector) {
        vectorsValue.put(id, vector);
        builder.addGraphNode(id, new VectorValueSupplier(() -> vector));
    }

    @Test
    public void testMask() {
        List<float[]> vectors1 = List.of(new float[]{0, 0}, new float[]{0, 0});

        var vectorsValue1 = new ListRandomAccessVectorValues(vectors1, 2);
        var graph1 = buildGraph(vectorsValue1);
        graph1.getNodes().forEachRemaining((int i) -> System.out.println(i + ": " + vectorsValue1.vectorValue(i)[0] + ", " + vectorsValue1.vectorValue(i)[1]));

//        List<float[]> vectors2 = List.of(new float[]{0, 0}, new float[]{0, 0}, new float[]{10, 10});
//        var vectorsValue2 = new ListRandomAccessVectorValues(vectors2, 2);
//        var graph2 = buildGraph(vectorsValue2);
//        graph2.getNodes().forEachRemaining((int i) -> System.out.println(i + ": " + vectorsValue2.vectorValue(i)[0] + ", " + vectorsValue2.vectorValue(i)[1]));

        var representation = graph1.getGraphRepresentation();
        System.out.println(representation);
        var searchVector = new float[]{1, 1};

        var result1 = searchEuclidean(searchVector, 2, graph1.getView(), vectorsValue1, Collections.emptySet());
        System.out.println("***********");
        System.out.println(result1);

//        var result2 = search(searchVector, 1, graph2.getView(), vectorsValue2, Collections.emptySet());
//        System.out.println("***********");
//        System.out.println(result2);
    }

    @Test
    public void testGraphBuilder() {
        var vectorsValue = new MutableListVectorValues(2);

        var builder = graphBuilder(vectorsValue, VectorSimilarityFunction.EUCLIDEAN);
        var graph = builder.getGraph();
        builder.addGraphNode(11, new VectorValueSupplier(() -> new float[]{0, 0}));
        vectorsValue.put(11, new float[]{0, 0});
        builder.addGraphNode(12, new VectorValueSupplier(() -> new float[]{1, 0}));
        vectorsValue.put(12, new float[]{1, 0});
        builder.addGraphNode(13, new VectorValueSupplier(() -> new float[]{0, 1}));
        vectorsValue.put(13, new float[]{0, 1});

        var graphRepresentation = graph.getGraphRepresentation();
        System.out.println(graphRepresentation);

        var searchVector = new float[]{1, 1};

        var result1 = searchEuclidean(searchVector, 1, graph.getView(), vectorsValue, Collections.emptySet());
        System.out.println("***********");
        System.out.println(result1);

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

        @Override
        public boolean isValueShared() {
            return false;
        }

        @Override
        public RandomAccessVectorValues<float[]> copy() {
            return new MutableListVectorValues(this);
        }
    }

    @Test
    public void testFilter() {
        int size = 11;
        int step = 10;
        List<float[]> vectors = new ArrayList<>();
        List<String> labels = new ArrayList<>();
        generateVectors(size, step, vectors, labels);
        vectors.add(new float[]{10, 10});
        labels.add("b");

        var vectorsValue = new ListRandomAccessVectorValues(vectors, 2);

        var graph = buildGraph(vectorsValue);

        var searchVector = new float[]{10, 10};
        var searchLabel = "a";

        System.out.println("search vector: " + searchVector[0] + " " + searchVector[1]);
        System.out.println("search label: " + searchLabel);
        var start = System.currentTimeMillis();
        List<Pair<Integer, Float>> result = searchEuclidean(
                searchVector,
                1,
                graph.getView(),
                new VectorProvider() {
                    @Override
                    public float[] get(int i) {
                        return vectorsValue.vectorValue(i);
                    }

                    @Override
                    public int size() {
                        return vectorsValue.size();
                    }
                },
                labels::get,
                s -> s.equals(searchLabel)
        );
        var end = System.currentTimeMillis();
        if (result.isEmpty()) {
            System.out.println("Not found");
        } else {
            var index = result.get(0).getFirst();
            System.out.println("index: " + index);
            System.out.println("score: " + result.get(0).getSecond());
            System.out.println("vector: " + vectors.get(index)[0] + " " + vectors.get(index)[1]);
            System.out.println("label: " + labels.get(index));
        }
        System.out.println("time: " + (end - start));
    }


    private IntStream rangeStep(int s, int f, int step) {
        return range(s, f).filter(i -> i % step == 0);
    }

    // return size * size list
    private void generateVectors(int size, int step, List<float[]> vectors, List<String> labels) {
        rangeStep(0, size, step).forEach(
                i -> rangeStep(0, size, step).forEach(j -> {
                    var vector = new float[]{i, j};
                    vectors.add(vector);
                    labels.add("a");
                })
        );
    }

    private GraphIndex buildGraph(RandomAccessVectorValues vectorsValue) {

        GraphIndexBuilder builder = new GraphIndexBuilder(
                vectorsValue,
                VectorEncoding.FLOAT32,
                VectorSimilarityFunction.EUCLIDEAN,
                3,
                50,
                0.5f,
                1.5f
        );

        return builder.build();
    }

    public static GraphIndexBuilder graphBuilder(RandomAccessVectorValues vectorsValue, VectorSimilarityFunction metric) {

        GraphIndexBuilder builder = new GraphIndexBuilder(
                vectorsValue,
                VectorEncoding.FLOAT32,
                metric,
                1, // number of neighbors = M * 2
                5,
                0.0f,
                1.5f
        );

        return builder;
    }

    public static List<Pair<Integer, Float>> searchEuclidean(float[] vector, int topK, GraphIndex.View<float[]> graphIndex, RandomAccessVectorValues<float[]> vectorsValue, Set<Integer> filter) {

        NodeSimilarity.ExactScoreFunction scoreFunction = (j) -> VectorSimilarityFunction.EUCLIDEAN.compare(vector, vectorsValue.vectorValue(j));

        GraphSearcher<float[]> searcher = new GraphSearcher.Builder<>(graphIndex).build();

        Bits mask = null;
        if (filter.isEmpty()) {
            mask = new Bits.MatchAllBits();
        } else {
            var fixedBitSet = new FixedBitSet(vectorsValue.size() + 1);
            filter.forEach(fixedBitSet::set);
            mask = fixedBitSet;
        }
        SearchResult searchResult = searcher.search(scoreFunction, null, topK, mask);
        List<Pair<Integer, Float>> resultsList = new ArrayList<>();
        for (SearchResult.NodeScore nodeScore : searchResult.getNodes()) {
            resultsList.add(new Pair<>(nodeScore.node, nodeScore.score));
        }
        return resultsList;
    }


    static List<Pair<Integer, Float>> search(
            float[] vector,
            int topK,
            GraphIndex.View<float[]> graphIndex,
            RandomAccessVectorValues<float[]> vectorsValue,
            Set<Integer> filter,
            VectorSimilarityFunction metric) {

        NodeSimilarity.ExactScoreFunction scoreFunction = (j) -> metric.compare(vector, vectorsValue.vectorValue(j));

        GraphSearcher<float[]> searcher = new GraphSearcher.Builder<>(graphIndex).build();

        Bits mask = null;
        if (filter.isEmpty()) {
            mask = new Bits.MatchAllBits();
        } else {
            var fixedBitSet = new FixedBitSet(vectorsValue.size() + 1);
            filter.forEach(fixedBitSet::set);
            mask = fixedBitSet;
        }
        SearchResult searchResult = searcher.search(scoreFunction, null, topK, mask);
        List<Pair<Integer, Float>> resultsList = new ArrayList<>();
        for (SearchResult.NodeScore nodeScore : searchResult.getNodes()) {
            resultsList.add(new Pair<>(nodeScore.node, nodeScore.score));
        }
        return resultsList;
    }

    static List<Pair<Integer, Float>> searchEuclidean(
            float[] vector,
            int topK,
            GraphIndex.View<float[]> graphIndex,
            VectorProvider vectorsValue,
            ValueaProvider metadataProvider,
            Function<String, Boolean> filter) {

        NodeSimilarity.ExactScoreFunction scoreFunction = (j) -> VectorSimilarityFunction.EUCLIDEAN.compare(vector, vectorsValue.get(j));

        GraphSearcher<float[]> searcher = new GraphSearcher.Builder<>(graphIndex).build();

        Bits mask = new InlineBits(metadataProvider, filter);

        SearchResult searchResult = searcher.search(scoreFunction, null, topK, mask);
        List<Pair<Integer, Float>> resultsList = new ArrayList<>();

        for (SearchResult.NodeScore nodeScore : searchResult.getNodes()) {
            resultsList.add(new Pair<>(nodeScore.node, nodeScore.score));
        }
        return resultsList;
    }

    @Test
    public void fixedBitSet() {
        FixedBitSet bitsSet = new FixedBitSet(100);
        bitsSet.set(63);
        bitsSet.set(64);
        bitsSet.set(65);
        var bits = bitsSet.getBits();
        Arrays.stream(bits).forEach(System.out::println);

        var result = bitsSet.get(1);
        System.out.println("result : " + result);
    }

    @Test
    public void sparseFixedBitSet() {
        SparseFixedBitSet bitsSet = new SparseFixedBitSet(100);
        bitsSet.set(0);
        bitsSet.set(64);
        bitsSet.set(65);
        var result = bitsSet.get(1);
        System.out.println("result : " + result);
    }

    @Test
    public void binaryOps() {
        var i = 5;
        var n = i >> 6;
        System.out.println(n);
    }

    interface ValueaProvider {
        String get(int i);
    }

    interface VectorProvider {
        float[] get(int i);

        int size();
    }

    static class InlineBits implements Bits {
        ValueaProvider valueProvider;
        Function<String, Boolean> filter;

        public InlineBits(ValueaProvider valueProvider, Function<String, Boolean> filter) {
            this.valueProvider = valueProvider;
            this.filter = filter;
        }

        @Override
        public boolean get(int index) {
            return filter.apply(valueProvider.get(index));
        }

        @Override
        public int length() {
            return 0;
        }
    }


    static class VectorValueSupplier implements RandomAccessVectorValues {

        Supplier<Object> supplier;

        public VectorValueSupplier(Supplier<Object> supplier) {
            this.supplier = supplier;
        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public int dimension() {
            return 0;
        }

        @Override
        public Object vectorValue(int targetOrd) {
            return supplier.get();
        }

        @Override
        public boolean isValueShared() {
            return false;
        }

        @Override
        public RandomAccessVectorValues copy() {
            return new VectorValueSupplier(supplier);
        }
    }


    private void muteException(Runnable run) {
        try {
            run.run();
        } catch (Exception e) {

        }
    }
}
