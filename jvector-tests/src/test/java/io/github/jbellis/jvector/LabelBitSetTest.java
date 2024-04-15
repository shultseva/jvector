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

}
