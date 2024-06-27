package io.github.jbellis.jvector;

import io.github.jbellis.jvector.graph.label.LabelsSet;
import io.github.jbellis.jvector.graph.label.impl.BitLabelSet;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LabelBitSetTest {

    @Test
    public void contains() {
        LabelsSet labelsSet = new BitLabelSet(new int[]{10, 30, 40, 20});
        assertTrue(labelsSet.contains(30));
        assertFalse(labelsSet.contains(31));
    }

    @Test
    public void getAll() {
        var expected = new int[]{10, 30, 40, 20};
        LabelsSet labelsSet = new BitLabelSet(expected);
        assertThat(labelsSet.get()).containsExactlyInAnyOrder(expected);
    }

    @Test
    public void containsAtLeastOne() {
        LabelsSet labelsSet1 = new BitLabelSet(new int[]{10, 30, 40, 20});
        LabelsSet labelsSet2 = new BitLabelSet(new int[]{11, 30, 44, 22});
        assertTrue(labelsSet1.containsAtLeastOne(labelsSet2));
    }

    @Test
    public void noIntersection() {
        LabelsSet labelsSet1 = new BitLabelSet(new int[]{10, 30, 40, 20});
        LabelsSet labelsSet2 = new BitLabelSet(new int[]{11, 33, 44, 22});
        assertFalse(labelsSet1.containsAtLeastOne(labelsSet2));
    }

    @Test
    public void containsAtLeastOneEmpty() {
        LabelsSet labelsSet1 = new BitLabelSet(new int[]{});
        LabelsSet labelsSet2 = new BitLabelSet(new int[]{11, 30, 44, 22});
        assertFalse(labelsSet1.containsAtLeastOne(labelsSet2));
    }

    @Test
    public void intersection_success() {
        LabelsSet labelsSet1 = new BitLabelSet(new int[]{10, 30, 40, 34, 20});
        LabelsSet labelsSet2 = new BitLabelSet(new int[]{11, 30, 44, 34, 22});
        var expected = new int[] {30, 34};
        var intersection1 = labelsSet1.intersect(labelsSet2).get();
        var intersection2 = labelsSet2.intersect(labelsSet1).get();
        assertThat(intersection1).containsExactlyInAnyOrder(expected);
        assertThat(intersection2).containsExactlyInAnyOrder(expected);
    }

    @Test
    public void intersection_empty() {
        LabelsSet labelsSet1 = new BitLabelSet(new int[]{10, 30, 40, 34, 20});
        LabelsSet labelsSet2 = new BitLabelSet(new int[]{11, 31, 44, 35, 22});
        var intersection1 = labelsSet1.intersect(labelsSet2).get();
        var intersection2 = labelsSet2.intersect(labelsSet1).get();
        assertThat(intersection1).isEmpty();
        assertThat(intersection2).isEmpty();
    }

    @Test
    public void included() {
        LabelsSet labelsSet1 = new BitLabelSet(new int[]{10, 30, 40, 34, 20});
        LabelsSet labelsSet2 = new BitLabelSet(new int[]{30, 34});
        assertThat(labelsSet1.include(labelsSet2)).isTrue();
        assertThat(labelsSet2.include(labelsSet1)).isFalse();
    }

    @Test
    public void includedEmpty() {
        LabelsSet labelsSet1 = new BitLabelSet(new int[]{10, 30, 40, 34, 20});
        LabelsSet labelsSet2 = new BitLabelSet(new int[]{});
        assertThat(labelsSet1.include(labelsSet2)).isTrue();
    }

    @Test
    public void notIncluded() {
        LabelsSet labelsSet1 = new BitLabelSet(new int[]{1, 2, 3, 4, 5});
        LabelsSet labelsSet2 = new BitLabelSet(new int[]{1, 2, 3, 4, 6});
        assertThat(labelsSet1.include(labelsSet2)).isFalse();
        assertThat(labelsSet2.include(labelsSet1)).isFalse();
    }
}
