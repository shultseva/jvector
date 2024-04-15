package io.github.jbellis.jvector.graph;

import io.github.jbellis.jvector.graph.label.EntriesGraphView;
import io.github.jbellis.jvector.graph.label.LabelsChecker;
import io.github.jbellis.jvector.util.BitSet;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.util.BoundedLongHeap;
import io.github.jbellis.jvector.util.GrowableBitSet;
import io.github.jbellis.jvector.util.GrowableLongHeap;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;

import static java.lang.Math.min;

public class LabeledGraphSearcher<T> {

    private final EntriesGraphView<T> view;
    private final BitSet visited;

    private final NodeQueue candidates;

    /**
     * Creates a new graph searcher.
     *
     * @param view
     * @param visited bit set that will track nodes that have already been visited
     */
    LabeledGraphSearcher(EntriesGraphView<T> view, BitSet visited) {
        this.visited = visited;
        this.view = view;
        this.candidates = new NodeQueue(new GrowableLongHeap(100), NodeQueue.Order.MAX_HEAP);
    }

    public SearchResult search(
            NodeSimilarity.ScoreFunction scoreFunction,
            LabelsChecker labelChecker,
            NodeSimilarity.ReRanker<T> reRanker,
            int topK,
            Bits acceptOrds
    ) {
        return searchInternal(scoreFunction, reRanker, topK, 0.0f, view.entryNodes(), acceptOrds, labelChecker);
    }

    SearchResult searchInternal(
            NodeSimilarity.ScoreFunction scoreFunction,
            NodeSimilarity.ReRanker<T> reRanker,
            int topK,
            float threshold,
            int[] entryPoints,
            Bits acceptOrds,
            LabelsChecker labelChecker
    ) {
        if (!scoreFunction.isExact() && reRanker == null) {
            throw new IllegalArgumentException("Either scoreFunction must be exact, or reRanker must not be null");
        }
        if (acceptOrds == null) {
            throw new IllegalArgumentException("Use MatchAllBits to indicate that all ordinals are accepted, instead of null");
        }

        prepareScratchState(view.size());
        var scoreTracker = threshold > 0 ? new ScoreTracker.NormalDistributionTracker(threshold) : new ScoreTracker.NoOpTracker();
        if (entryPoints.length == 0) {
            return new SearchResult(new SearchResult.NodeScore[0], visited, 0);
        }

        acceptOrds = Bits.intersectionOf(acceptOrds, view.liveNodes());

        // Threshold callers (and perhaps others) will be tempted to pass in a huge topK.
        // Let's not allocate a ridiculously large heap up front in that scenario.
        var resultsQueue = new NodeQueue(new BoundedLongHeap(min(1024, topK), topK), NodeQueue.Order.MIN_HEAP);
        Map<Integer, T> vectorsEncountered = scoreFunction.isExact() ? null : new java.util.HashMap<>();
        int numVisited = 0;

        for (int ep : entryPoints) {
            if (!labelChecker.hasLables(ep)) {
                continue;
            }
            if (visited.getAndSet(ep)) {
                continue;
            }
            float score = scoreFunction.similarityTo(ep);

            numVisited++;
            candidates.push(ep, score);
        }

        // A bound that holds the minimum similarity to the query vector that a candidate vector must
        // have to be considered.
        float minAcceptedSimilarity = Float.NEGATIVE_INFINITY;

        while (candidates.size() > 0 && !resultsQueue.incomplete()) {
            // done when best candidate is worse than the worst result so far
            float topCandidateScore = candidates.topScore();
            if (topCandidateScore < minAcceptedSimilarity) {
                break;
            }

            // periodically check whether we're likely to find a node above the threshold in the future
            if (scoreTracker.shouldStop(numVisited)) {
                break;
            }

            // add the top candidate to the resultset
            int topCandidateNode = candidates.pop();
            if (acceptOrds.get(topCandidateNode)
                    && topCandidateScore >= threshold
                    && resultsQueue.push(topCandidateNode, topCandidateScore)) {
                if (resultsQueue.size() >= topK) {
                    minAcceptedSimilarity = resultsQueue.topScore();
                }
                if (!scoreFunction.isExact()) {
                    vectorsEncountered.put(topCandidateNode, view.getVector(topCandidateNode));
                }
            }

            // add its neighbors to the candidates queue
            for (var it = view.getNeighborsIterator(topCandidateNode); it.hasNext(); ) {
                int friendOrd = it.nextInt();
                if (!labelChecker.hasLables(friendOrd)) {
                    continue;
                }
                if (visited.getAndSet(friendOrd)) {
                    continue;
                }
                numVisited++;

                float friendSimilarity = scoreFunction.similarityTo(friendOrd);
                scoreTracker.track(friendSimilarity);
                if (friendSimilarity >= minAcceptedSimilarity) {
                    candidates.push(friendOrd, friendSimilarity);
                }
            }
        }

        assert resultsQueue.size() <= topK;
        SearchResult.NodeScore[] nodes = extractScores(scoreFunction, reRanker, resultsQueue, vectorsEncountered);
        return new SearchResult(nodes, visited, numVisited);
    }

    private static <T> SearchResult.NodeScore[] extractScores(NodeSimilarity.ScoreFunction sf,
                                                              NodeSimilarity.ReRanker<T> reRanker,
                                                              NodeQueue resultsQueue,
                                                              Map<Integer, T> vectorsEncountered) {
        SearchResult.NodeScore[] nodes;
        if (sf.isExact()) {
            nodes = new SearchResult.NodeScore[resultsQueue.size()];
            for (int i = nodes.length - 1; i >= 0; i--) {
                var nScore = resultsQueue.topScore();
                var n = resultsQueue.pop();
                nodes[i] = new SearchResult.NodeScore(n, nScore);
            }
        } else {
            nodes = resultsQueue.nodesCopy(i -> reRanker.similarityTo(i, vectorsEncountered));
            Arrays.sort(nodes, 0, resultsQueue.size(), Comparator.comparingDouble((SearchResult.NodeScore nodeScore) -> nodeScore.score).reversed());
        }
        return nodes;
    }

    private void prepareScratchState(int capacity) {
        candidates.clear();
        if (visited.length() < capacity) {
            // this happens during graph construction; otherwise the size of the vector values should
            // be constant, and it will be a SparseFixedBitSet instead of FixedBitSet
            if (!(visited instanceof GrowableBitSet)) {
                throw new IllegalArgumentException(
                        String.format("Unexpected visited type: %s. Encountering this means that the graph changed " +
                                        "while being searched, and the Searcher was not built withConcurrentUpdates()",
                                visited.getClass().getName()));
            }
            // else GrowableBitSet knows how to grow itself safely
        }
        visited.clear();
    }



    public static class LabelSearchOption {
        private int[] labels;

        private Condition condition;

        public LabelSearchOption(int[] labels, Condition condition) {
            this.labels = labels;
            this.condition = condition;
        }

        public static LabelSearchOption createAtLeastOne(int[] labels) {
            return new LabelSearchOption(labels, Condition.AT_LEAST_ONE);
        }

        public static LabelSearchOption createFitAll(int[] labels) {
            return new LabelSearchOption(labels, Condition.ALL);
        }

        public int[] getLabels() {
            return labels;
        }

        public Condition getCondition() {
            return condition;
        }

        public enum Condition {
            ALL,
            AT_LEAST_ONE
        }
    }
}
