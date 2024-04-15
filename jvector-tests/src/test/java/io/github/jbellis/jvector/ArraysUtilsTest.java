package io.github.jbellis.jvector;

import io.github.jbellis.jvector.graph.label.ArraysUtils;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ArraysUtilsTest {

    @Test
    public void hasCommon() {
        int[] a = new int[]{1,3,5,7,9};
        int[] b = new int[]{2,4,5,6,8};
        assertTrue(ArraysUtils.hasCommon(a, b));
    }

    @Test
    public void hasCommon2() {
        int[] a = new int[]{1,2};
        int[] b = new int[]{2};
        assertTrue(ArraysUtils.hasCommon(a, b));
    }

    @Test
    public void hasNoCommon() {
        int[] a = new int[]{1,3,5,7,9};
        int[] b = new int[]{2,4,6,8};
        assertFalse(ArraysUtils.hasCommon(a, b));
    }
}
