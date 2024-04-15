package io.github.jbellis.jvector.graph;

import io.github.jbellis.jvector.SimpleTest;
import io.github.jbellis.jvector.graph.label.EntriesGraphView;
import io.github.jbellis.jvector.graph.label.LabelsChecker;
import io.github.jbellis.jvector.graph.label.LabelsSet;
import io.github.jbellis.jvector.graph.label.MutableAccessVectorLabels;
import io.github.jbellis.jvector.graph.label.RandomAccessVectorLabels;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.util.GrowableBitSet;
import io.github.jbellis.jvector.vector.VectorEncoding;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import static io.github.jbellis.jvector.graph.label.impl.BitLabelSet.asLabelSet;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;

public class LabeledGraphTest {
    int[] nodeLabelArray1 = new int[]{1};
    int[] nodeLabelArray2 = new int[]{2};
    float[][] vectors = new float[][]{new float[]{0, 1}, new float[]{1, 0}, new float[]{0, 0}, new float[]{1, 1}, new float[]{2, 2}};
    Integer[] ids = new Integer[]{11, 12, 13, 14, 15};


    @Test
    public void testOneLabel() {
        int[][] labels = new int[][]{nodeLabelArray1, nodeLabelArray1, nodeLabelArray1, nodeLabelArray1, nodeLabelArray1};

        var vectorsValue = new SimpleTest.MutableListVectorValues(vectors[0].length);
        var vectorsLabels = new MutableAccessVectorLabels();

        var builder = graphBuilder(vectorsValue, vectorsLabels);
        var graph = builder.getGraph();
        for (int i = 0; i < ids.length; i++) {
            vectorsValue.put(ids[i], vectors[i]);
            vectorsLabels.put(ids[i], labels[i]);
            builder.addGraphNode(ids[i], vectorsValue, vectorsLabels);
        }

        printGraphInfo(graph);

        assertThat(graph.entries()).hasSize(1);
        var searchVector = new float[]{1, 1};

        var searchResult = search(searchVector, 4, graph.getView(), vectorsValue, vectorsLabels, 1);
        System.out.println("Search result: " + searchResult);
        assertThat(searchResult).hasSize(4);
    }

    @Test
    public void testNoIntersectionLabels() {
        int[][] labels = new int[][]{nodeLabelArray1, nodeLabelArray1, nodeLabelArray1, nodeLabelArray2, nodeLabelArray2};

        var vectorsValue = new SimpleTest.MutableListVectorValues(vectors[0].length);
        var vectorsLabels = new MutableAccessVectorLabels();

        var builder = fillDefaultGraph(vectorsValue, vectorsLabels, labels);
        var graph = builder.getGraph();

        printGraphInfo(graph);

        assertThat(graph.entries()).hasSize(2);

        var searchVector = new float[]{1, 1};

        Map<Integer, NodeInfo> searchResultLabel1 = search(searchVector, 4, graph.getView(), vectorsValue, vectorsLabels, 1);
        assertThat(searchResultLabel1.keySet()).containsExactlyInAnyOrder(ids[0], ids[1], ids[2]);

        Map<Integer, NodeInfo> searchResultLabel2 = search(searchVector, 4, graph.getView(), vectorsValue, vectorsLabels, 2);
        assertThat(searchResultLabel2.keySet()).containsExactlyInAnyOrder(ids[3], ids[4]);

        Map<Integer, NodeInfo> searchResultLabelAll = search(searchVector, 10, graph.getView(), vectorsValue, vectorsLabels, new int[]{1, 2});
        assertThat(searchResultLabelAll.keySet()).containsExactlyInAnyOrderElementsOf(Arrays.asList(ids));
    }

    @Test
    public void testIntersectionLabels() {
        int[][] labels = new int[][]{nodeLabelArray1, nodeLabelArray1, new int[]{1, 2}, nodeLabelArray2, nodeLabelArray2};

        var vectorsValue = new SimpleTest.MutableListVectorValues(vectors[0].length);
        var vectorsLabels = new MutableAccessVectorLabels();

        var builder = fillDefaultGraph(vectorsValue, vectorsLabels, labels);
        var graph = builder.getGraph();

        printGraphInfo(graph);

        assertThat(graph.entries()).hasSize(2);

        var searchVector = new float[]{1, 1};

        Map<Integer, NodeInfo> searchResultLabel1 = search(searchVector, 4, graph.getView(), vectorsValue, vectorsLabels, 1);
        assertThat(searchResultLabel1.keySet()).containsExactlyInAnyOrder(ids[0], ids[1], ids[2]);

        Map<Integer, NodeInfo> searchResultLabel2 = search(searchVector, 4, graph.getView(), vectorsValue, vectorsLabels, 2);
        assertThat(searchResultLabel2.keySet()).containsExactlyInAnyOrder(ids[2], ids[3], ids[4]);

        Map<Integer, NodeInfo> searchResultLabelAll = search(searchVector, 10, graph.getView(), vectorsValue, vectorsLabels, new int[]{1, 2});
        assertThat(searchResultLabelAll.keySet()).containsExactlyInAnyOrderElementsOf(Arrays.asList(ids));
    }


    @Test
    public void testTheSameTwoLabel() {
        var label = new int[]{1, 2};
        int[][] labels = new int[][]{label, label, label, label, label};

        var vectorsValue = new SimpleTest.MutableListVectorValues(vectors[0].length);
        var vectorsLabels = new MutableAccessVectorLabels();

        var builder = fillDefaultGraph(vectorsValue, vectorsLabels, labels);
        var graph = builder.getGraph();

        printGraphInfo(graph);

        assertThat(graph.entries()).hasSize(2);

        var searchVector = new float[]{1, 1};

        Map<Integer, NodeInfo> searchResultLabel1 = search(searchVector, 5, graph.getView(), vectorsValue, vectorsLabels, 1);
        assertThat(searchResultLabel1.keySet()).containsExactlyInAnyOrderElementsOf(Arrays.asList(ids));

        Map<Integer, NodeInfo> searchResultLabel2 = search(searchVector, 5, graph.getView(), vectorsValue, vectorsLabels, 2);
        assertThat(searchResultLabel2.keySet()).containsExactlyInAnyOrderElementsOf(Arrays.asList(ids));
    }


    @Test
    @Ignore
    public void testBigCellsGraph() {
        int size = 1_000;
        List<int[]> labels = range(0, size).mapToObj(i -> new int[]{i, (i + 1) % size}).collect(Collectors.toList());

        var vectorsValue = new SimpleTest.MutableListVectorValues(2);
        var vectorsLabels = new MutableAccessVectorLabels();

        var builder = createCellsTestGraph(vectorsValue, vectorsLabels, size, labels);
        var graph = builder.getGraph();
        assertThat(getReachableNodes(graph, graph.entries()[0])).hasSize(size * size);

       // printGraphInfo(graph);

        assertThat(graph.entries()).hasSize(labels.size());

        var searchVector = new float[]{5, 10};

        var kTop = 10 * size;
        Map<Integer, NodeInfo> searchResultLabel1 = search(searchVector, kTop, graph.getView(), vectorsValue, vectorsLabels, 1);
        assertThat(searchResultLabel1.keySet()).hasSize(2 * size);

        Map<Integer, NodeInfo> searchResultLabel2 = search(searchVector, kTop, graph.getView(), vectorsValue, vectorsLabels, new int[] {1, 2});
        assertThat(searchResultLabel2.keySet()).hasSize(3 * size);

        Map<Integer, NodeInfo> searchResultLabel3 = search(searchVector, kTop, graph.getView(), vectorsValue, vectorsLabels, new int[] {1, 3});
        assertThat(searchResultLabel3.keySet()).hasSize(4 * size);
    }


    @Test
    public void testRandomGraph() {
        int size = 100_000;

        int dim = 2;
        int biggestAllowedCoordinateValue = 10_000; // from 0 to biggestAllowedCoordinateValue

        int maxNumberOfLabelsPerNode = 50;
        int biggestAllowedLabelValue = 100; // from 0 to biggestLabel

        int[] ids = range(0, size).map(i -> 1_000 + i).toArray();
        List<int[]> labels = range(0, size).mapToObj(i -> generateRandomLabels(maxNumberOfLabelsPerNode, biggestAllowedLabelValue)).collect(Collectors.toList());
        List<float[]> coordinates = range(0, size).mapToObj(i -> generateRandomVector(dim, biggestAllowedCoordinateValue)).collect(Collectors.toList());

        var vectorsValue = new SimpleTest.MutableListVectorValues(2);
        var vectorsLabels = new MutableAccessVectorLabels();

        var builder = fillGraph(vectorsValue, vectorsLabels, ids, coordinates, labels);

        var graph = builder.getGraph();

 //       printLabelInfo(labels, ids);

        var searchVector = new float[]{100, 100};
        int[] searchLabels = new int[] {1, 2};
        var kTop = 5;
        Map<Integer, NodeInfo> searchResultLabel = search(searchVector, kTop, graph.getView(), vectorsValue, vectorsLabels, searchLabels);
        assertThat(searchResultLabel).allSatisfy((k, node) -> assertThat(node.labels).containsAnyOf(searchLabels));
        System.out.println(searchResultLabel);
    }

    private float[] generateRandomVector(int dim, int max) {
        Random random = new Random();
        float[] vector = new float[dim];
        for (int c = 0; c < dim; c++) {
            vector[c] = random.nextInt(max);
        }
        return vector;
    }
    private int[] generateRandomLabels(int maxNumberOfLabels, int upperBoundary) {
        Random random = new Random();
        int numberOfLabels = 1 + random.nextInt(maxNumberOfLabels); // at least one label should have
        Set<Integer> labels = new HashSet<>();
        for (int c = 0; c < numberOfLabels; c++) {
            labels.add(random.nextInt(upperBoundary));
        }
        return labels.stream().mapToInt(i -> i).toArray();
    }

    private void printGraphInfo(LabeledOnHeapGraphIndex<float[]> graph) {
        var graphRepresentation = graph.getGraphRepresentation();
        var entryPoints = graph.entries();

        System.out.println("Graph: " + graphRepresentation);
        System.out.println("Entry points: " + Arrays.stream(entryPoints).boxed().collect(Collectors.toList()));
        for (int entry : entryPoints) {
            System.out.println("Reachable nodes from " + entry + ": " + getReachableNodes(graph, entry));
        }
    }

    private void printLabelInfo(List<int[]> labels, int[] ids) {
        System.out.println("Vector labels info");
        for(int i = 0; i < ids.length; i++) {
            System.out.println("Node: " + ids[i] + " labels: " + Arrays.toString(labels.get(i)));
        }
    }
    private Set<Integer> getReachableNodes(LabeledOnHeapGraphIndex<float[]> graph, int entry) {
        Set<Integer> visited = new HashSet<>();
        var map = graph.getGraphRepresentation();
        Queue<Integer> queue = new LinkedList<>();
        queue.add(entry);
        while (!queue.isEmpty()) {
            var node = queue.poll();
            var nbrs = map.get(node);
            for (var nbr : nbrs) {
                if (visited.add(nbr)) {
                    queue.add(nbr);
                }
            }
        }
        return visited;
    }

    private LabeledGraphIndexBuilder<float[]> createCellsTestGraph(
            SimpleTest.MutableListVectorValues vectorValues,
            MutableAccessVectorLabels vectorLabels,
            int size,
            List<int[]> labels
    ) {
        assert  labels.size() == size;
        int[] cellsIds = new int[size * size];
        LabeledGraphIndexBuilder<float[]> builder = graphBuilder(vectorValues, vectorLabels);
        for (int i = 0; i < cellsIds.length; i++) {
            if(i % size == 0) {
                var shuffle = labels.get(0);
                labels.remove(0);
                labels.add(size - 1, shuffle);
            }
            var id = 100_000 + i;
            cellsIds[i] = id;
            vectorValues.put(id, new float[]{(float) i / size, i % size});
            vectorLabels.put(id, labels.get(i % size));
            builder.addGraphNode(id, vectorValues, vectorLabels);
        }
        return builder;
    }

    private LabeledGraphIndexBuilder<float[]> fillDefaultGraph(
            SimpleTest.MutableListVectorValues vectorValues,
            MutableAccessVectorLabels vectorLabels,
            int[][] labels
    ) {
        LabeledGraphIndexBuilder<float[]> builder = graphBuilder(vectorValues, vectorLabels);
        for (int i = 0; i < ids.length; i++) {
            vectorValues.put(ids[i], vectors[i]);
            vectorLabels.put(ids[i], labels[i]);
            builder.addGraphNode(ids[i], vectorValues, vectorLabels);
        }
        return builder;
    }

    private LabeledGraphIndexBuilder<float[]> fillGraph(
            SimpleTest.MutableListVectorValues vectorValues,
            MutableAccessVectorLabels vectorLabels,
            int[] ids,
            List<float[]> vectors,
            List<int[]> labels
    ) {
        LabeledGraphIndexBuilder<float[]> builder = graphBuilder(vectorValues, vectorLabels);
        for (int i = 0; i < ids.length; i++) {
            vectorValues.put(ids[i], vectors.get(i));
            vectorLabels.put(ids[i], labels.get(i));
            builder.addGraphNode(ids[i], vectorValues, vectorLabels);
        }
        return builder;
    }

    private LabeledGraphIndexBuilder<float[]> graphBuilder(RandomAccessVectorValues<float[]> vectorsValue, RandomAccessVectorLabels<LabelsSet> vectorLabels) {

        return new LabeledGraphIndexBuilder<>(
                vectorsValue,
                vectorLabels,
                VectorEncoding.FLOAT32,
                VectorSimilarityFunction.EUCLIDEAN,
                20,
                50,
                0.5f,
                1.5f
        );
    }

    static Map<Integer, NodeInfo> search(
            float[] vector,
            int topK,
            EntriesGraphView<float[]> graphView,
            RandomAccessVectorValues<float[]> vectorsValue,
            RandomAccessVectorLabels<LabelsSet> vectorLabels,
            Integer label
    ) {
        return search(vector, topK, graphView, vectorsValue, vectorLabels, new int[]{label});
    }

    static Map<Integer, NodeInfo> search(
            float[] vector,
            int topK,
            EntriesGraphView<float[]> graphView,
            RandomAccessVectorValues<float[]> vectorsValue,
            RandomAccessVectorLabels<LabelsSet> vectorLabels,
            int[] labels
    ) {

        NodeSimilarity.ExactScoreFunction scoreFunction = (j) -> VectorSimilarityFunction.EUCLIDEAN.compare(vector, vectorsValue.vectorValue(j));

        var size = graphView.getIdUpperBound();
        LabeledGraphSearcher<float[]> searcher = new LabeledGraphSearcher<>(graphView, new GrowableBitSet(size));

        Bits mask = new Bits.MatchAllBits();
        LabelsSet labelsSet = asLabelSet(labels);
        LabelsChecker checker = node -> vectorLabels.vectorLabels(node).containsAtLeastOne(labelsSet);
        SearchResult searchResult = searcher.search(scoreFunction, checker, null, topK, mask);
        Map<Integer, NodeInfo> mapResult = new HashMap<>();
        for (SearchResult.NodeScore nodeScore : searchResult.getNodes()) {
            mapResult.put(nodeScore.node, new NodeInfo(nodeScore.score, vectorsValue.vectorValue(nodeScore.node), vectorLabels.vectorLabels(nodeScore.node)));
        }
        return mapResult;
    }

    public static class NodeInfo {
        Float score;

        float[] vector;

        int[] labels;

        public NodeInfo(Float score, float[] vector, LabelsSet labels) {
            this.score = score;
            this.vector = vector;
            this.labels = labels.get();
        }

        @Override
        public String toString() {
            return "NodeInfo{" +
                    "score=" + score +
                    ", vector=" + Arrays.toString(vector) +
                    ", labels=" + Arrays.toString(labels) +
                    '}';
        }
    }
}
