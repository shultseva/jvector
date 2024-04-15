package io.github.jbellis.jvector;

import io.github.jbellis.jvector.graph.label.Int2SortedIntArrayHashMap;
import org.junit.Test;

import java.util.function.BiFunction;

import static java.util.stream.IntStream.range;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

public class Int2IntArrayHashMapTest {

    BiFunction<Integer, Integer, int[]> generateWithTheSameIndex = (Integer index, Integer limit) -> range(1, 1_000)
            .filter(i -> Int2SortedIntArrayHashMap.intHash(i, 15) == index)
            .limit(limit)
            .toArray();

    @Test
    public void putGet() {
        Int2SortedIntArrayHashMap map = new Int2SortedIntArrayHashMap(10, 0.8);
        var keys = generateWithTheSameIndex.apply(1, 5);
        var keys2 = generateWithTheSameIndex.apply(2, 5);

        for(int key: keys) {
            map.put(key, new int[]{key});
        }
        for(int key: keys2) {
            map.put(key, new int[]{key});
        }

        for(int key: keys2) {
            var actual = map.get(key);
            assertArrayEquals(actual, new int[]{key});
        }
        for(int key: keys) {
            var actual = map.get(key);
            assertArrayEquals(actual, new int[]{key});
        }
    }

    @Test
    public void increaseCapacityTest() {
        Int2SortedIntArrayHashMap map = new Int2SortedIntArrayHashMap(3, 0.8);
        var keys = generateWithTheSameIndex.apply(1, 1_000);
        var keys2 = generateWithTheSameIndex.apply(2, 1_000);

        for(int key: keys) {
            map.put(key, new int[]{key});
        }
        for(int key: keys2) {
            map.put(key, new int[]{key});
        }

        for(int key: keys2) {
            var actual = map.get(key);
            assertArrayEquals(actual, new int[]{key});
        }
        for(int key: keys) {
            var actual = map.get(key);
            assertArrayEquals(actual, new int[]{key});
        }
    }
}
