package io.github.jbellis.jvector.graph;

import io.github.jbellis.jvector.util.BitSet;
import io.github.jbellis.jvector.util.DocIdSetIterator;

@Deprecated
public class LabeledConcurrentNeighborSet extends ConcurrentNeighborSet {

    private final NodeSimilarity similarity;
    public LabeledConcurrentNeighborSet(int nodeId, int maxConnections, NodeSimilarity similarity) {
        this(nodeId, maxConnections, similarity, 1.0f);
    }

    public LabeledConcurrentNeighborSet(int nodeId, int maxConnections, NodeSimilarity similarity, float alpha) {
        this(nodeId, maxConnections, similarity, alpha, new NodeArray(maxConnections));
    }

    LabeledConcurrentNeighborSet(int nodeId, int maxConnections, NodeSimilarity similarity, float alpha, NodeArray neighbors) {
        super(nodeId, maxConnections, similarity, alpha, neighbors);
        this.similarity = similarity;
    }

    // is the candidate node with the given score closer to the base node than it is to any of the
    // existing neighbors
    protected boolean isDiverse(
            int node, float score, NodeArray others, BitSet selected, float alpha) {
        if (others.size() == 0) {
            return true;
        }

        var scoreProvider = similarity.scoreProvider(node);
        for (int i = selected.nextSetBit(0); i != DocIdSetIterator.NO_MORE_DOCS; i = selected.nextSetBit(i + 1)) {
            int otherNode = others.node()[i];
            if (node == otherNode) {
                break;
            }
            if (scoreProvider.similarityTo(otherNode) > score * alpha) {
                return false;
            }

            // nextSetBit will error out if we're at the end of the bitset, so check this manually
            if (i + 1 >= selected.length()) {
                break;
            }
        }
        return true;
    }
}
