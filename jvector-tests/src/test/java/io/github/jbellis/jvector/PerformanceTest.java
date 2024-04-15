package io.github.jbellis.jvector;

import io.github.jbellis.jvector.graph.GraphIndexBuilder;
import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.vector.VectorEncoding;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.IntStream;

import static io.github.jbellis.jvector.SimpleTest.searchEuclidean;
import static java.util.stream.IntStream.range;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class PerformanceTest {

    ForkJoinPool pool = new ForkJoinPool(4);

/*
Add parallel: size = 10000, dim = 10, coordinated = [0 -  1000]. Total time: 477 ms
Add parallel: size = 100000, dim = 10, coordinated = [0 -  10000]. Total time: 1391 ms
Add parallel: size = 1000000, dim = 10, coordinated = [0 -  10000]. Total time: 20724 ms
Add by one: size = 10000, dim = 10, coordinated = [0 -  1000]. Total time: 622 ms
Add by one: size = 100000, dim = 10, coordinated = [0 -  10000]. Total time: 9062 ms
Add by one: size = 1000000, dim = 10, coordinated = [0 -  100000]. Total time: 158447 ms
 */

    @Test
    public void testPerformance() {
        byParallel(10_000, 20, 0, 1_000);
        byParallel(100_000, 20, 0, 10_000);
      //  byParallel(1_000_000, 20, 0, 10_000);

        byOne(10_000, 20, 0, 1_000);
        byOne(100_000, 20, 0, 10_000);
      //  byOne(1_000_000, 20, 0, 100_000);
    }

    @Test
    public void testPerformanceWithInit() {
        //  byOneWithInit(10_000, 10_000, 20, 0, 1_000);
        byOneWithInit(2_000_000, 100_000, 20, 0, 10_000);
    }

    public void byOne(int size, int dim, int min, int max) {
        var vectors = generateData(size, dim, min, max);

        var builder = createBuilder(vectors);

        var time = measure(() -> addByOne(builder, vectors));

        System.out.println(String.format("Add by one: size = %d, dim = %d, coordinated = [%d -  %d]. Total time: %d ms ", size, dim, min, max, time));

        assertEquals(size, builder.getGraph().size());
    }

    public void byOneWithInit(int initSize, int size, int dim, int min, int max) {
        var vectors = generateData(initSize, dim, min, max);

        var builder = createBuilder(vectors);
        var buildTime = measure(() -> builder.build());
        System.out.println("Build finished. Took " + buildTime + " ms");
        assertEquals(initSize, builder.getGraph().size());

        generateDataTo(vectors, initSize, size, dim, min, max);

        var time = measure(() -> addByOne(builder, vectors, initSize, initSize + size));

        System.out.println(String.format("Add by one: initial size: %s, size = %d, dim = %d, coordinated = [%d -  %d]. Total time: %d ms ", initSize, size, dim, min, max, time));

        assertEquals(size + initSize, builder.getGraph().size());
    }

    public void byParallel(int size, int dim, int min, int max) {
        var vectors = generateData(size, dim, min, max);

        var builder = createBuilder(vectors);

        var time = measure(() -> addParallel(builder, vectors));

        System.out.println(String.format("Add parallel: size = %d, dim = %s, coordinated = [%s -  %s]. Total time: %s ms", size, dim, min, max, time));

        assertEquals(size, builder.getGraph().size());
    }

    public MutableAccessVectorValues generateData(int initialSize, int dim, int min, int max) {
        var vectorsValue = new MutableAccessVectorValues(Collections.emptyList(), dim);
        Random r = new Random();
        range(0, initialSize).forEach(i -> {
            var vector = new float[dim];
            for (int c = 0; c < dim; c++) {
                vector[c] = r.nextInt(max);
            }
            vectorsValue.put(vector);
        });
        return vectorsValue;
    }

    public MutableAccessVectorValues generateDataTo(MutableAccessVectorValues vectorsValue, int fromIndex, int size, int dim, int min, int max) {
        Random r = new Random();
        range(fromIndex, fromIndex + size).forEach(i -> {
            var vector = new float[dim];
            for (int c = 0; c < dim; c++) {
                vector[c] = r.nextInt(max);
            }
            vectorsValue.put(vector);
        });
        return vectorsValue;
    }

    public GraphIndexBuilder createBuilder(MutableAccessVectorValues vectorsValue) {
        GraphIndexBuilder builder = new GraphIndexBuilder(
                vectorsValue,
                VectorEncoding.FLOAT32,
                VectorSimilarityFunction.EUCLIDEAN,
                40, // number of neighbors = M * 2
                50,
                0.0f,
                1.5f
        );
        return builder;
    }


    public void addByOne(GraphIndexBuilder builder, MutableAccessVectorValues vectors) {
        int size = vectors.size();
        IntStream.range(0, size).forEach(i -> builder.addGraphNode(i, vectors));
    }

    public void addByOne(GraphIndexBuilder builder, MutableAccessVectorValues vectors, int from, int to) {
        IntStream.range(from, to).forEach(i -> builder.addGraphNode(i, vectors));
    }

    public void addParallel(GraphIndexBuilder builder, MutableAccessVectorValues vectors) {
        int size = vectors.size();
        try {
            pool.submit(
                    () -> IntStream.range(0, size).parallel().forEach(i -> builder.addGraphNode(i, vectors))
            ).get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public long measure(Runnable runnable) {
        var start = System.currentTimeMillis();
        runnable.run();
        return System.currentTimeMillis() - start;
    }


    public void print(float[] vs) {
        var str = "";
        for (var v : vs) {
            str += ", " + v;
        }
        System.out.println(str);
    }

    public class MutableAccessVectorValues implements RandomAccessVectorValues<float[]> {

        private final List<float[]> vectors;
        private final int dimension;

        /**
         * Construct a new instance of {@link io.github.jbellis.jvector.graph.ListRandomAccessVectorValues}.
         *
         * @param vectors   a (potentially mutable) list of float vectors.
         * @param dimension the dimension of the vectors.
         */
        public MutableAccessVectorValues(List<float[]> vectors, int dimension) {
            this.vectors = new ArrayList<>(vectors);
            this.dimension = dimension;
        }

        public void put(float[] v) {
            vectors.add(v);
        }

        @Override
        public int size() {
            return vectors.size();
        }

        @Override
        public int dimension() {
            return dimension;
        }

        @Override
        public float[] vectorValue(int targetOrd) {
            return vectors.get(targetOrd);
        }

        @Override
        public boolean isValueShared() {
            return false;
        }

        @Override
        public MutableAccessVectorValues copy() {
            return this;
        }
    }
}
