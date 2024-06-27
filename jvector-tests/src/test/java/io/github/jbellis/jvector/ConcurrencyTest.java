package io.github.jbellis.jvector;

import io.github.jbellis.jvector.graph.GraphIndexBuilder;
import io.github.jbellis.jvector.graph.GraphSearcher;
import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static io.github.jbellis.jvector.CustomTestUtils.graphBuilder;
import static org.junit.Assert.assertEquals;

public class ConcurrencyTest {

    AtomicBoolean isAlive = new AtomicBoolean(true);
    volatile GraphIndexBuilder<float[]> builder;
    volatile CustomTestUtils.MutableListVectorValues vectorsValue;
    volatile GraphSearcher<float[]> searcher;

    ExecutorService service = Executors.newFixedThreadPool(5);

    AtomicInteger searchIndex = new AtomicInteger();
    AtomicInteger insertIndex = new AtomicInteger();
    AtomicInteger deleteIndex = new AtomicInteger();
    AtomicInteger cleanupIndex = new AtomicInteger(1);

    Callable<Void> search = () -> {
        while (isAlive.get()) {
            var id = new Random().nextInt(insertIndex.get());
            var vector = vectorsValue.vectorValue(id);

            var result = CustomTestUtils.search(vector, 10, searcher, vectorsValue, VectorSimilarityFunction.EUCLIDEAN);
            Assertions.assertEquals(10, result.size());
        }
        return null;
    };

    Callable<Void> insert = new Callable<>() {
        @Override
        public Void call() {
            while (isAlive.get()) {
                var id = insertIndex.getAndIncrement();
                var vector = new float[]{id, id + 1};
                CustomTestUtils.addNode(vectorsValue, builder, id, vector);
            }
            return null;
        }
    };

    Callable<Void> delete = () -> {
        while (isAlive.get()) {
            var id = new Random().nextInt(insertIndex.get());
            builder.markNodeDeleted(id);
            deleteIndex.getAndIncrement();
        }
        return null;
    };

    Callable<Void> cleanup = () -> {
        while (isAlive.get()) {
            if (deleteIndex.get() / cleanupIndex.get() > 100) {
                builder.cleanup();
            }
        }
        return null;
    };

    void setup() {
        builder.getGraph().getView();
        vectorsValue = new CustomTestUtils.MutableListVectorValues(2);
        builder = graphBuilder(vectorsValue, VectorSimilarityFunction.EUCLIDEAN, 2, 10);
        searcher = new GraphSearcher.Builder<>(builder.getGraph().getView()).withConcurrentUpdates().build();
    }

    @Test
    public void test() throws InterruptedException, ExecutionException {
        setup();
        service.submit(insert);
        Thread.sleep(1000);
        //service.submit(delete);
        service.submit(search).get();

        Thread.sleep(1000);

        isAlive.set(false);

        service.shutdown();

        System.out.println("insert: " + insertIndex.get());
        System.out.println("search: " + searchIndex.get());
        System.out.println("delete: " + deleteIndex.get());
    }


    private void runSafe(Runnable runnable) {
        try {
            runnable.run();
        } catch (Exception e) {
            System.out.println(e);
        }
    }

}
