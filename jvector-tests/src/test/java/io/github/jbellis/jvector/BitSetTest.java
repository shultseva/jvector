package io.github.jbellis.jvector;


import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

public class BitSetTest {

    @Test
    public void test() {
        Integer a = 1000;
        Integer b = 1000;
        Assertions.assertTrue(a.equals(b));
        Assertions.assertTrue(a.intValue() == b.intValue());
        Assertions.assertFalse(a.intValue() != b.intValue());
    }

    @Test
    public void test2() {
        var similarityFunction = VectorSimilarityFunction.DOT_PRODUCT;
        similarityFunction.compare(new float[] {1, 2}, null);
    }
}
