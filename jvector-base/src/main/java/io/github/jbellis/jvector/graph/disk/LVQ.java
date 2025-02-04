/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.jbellis.jvector.graph.disk;

import io.github.jbellis.jvector.disk.RandomAccessReader;
import io.github.jbellis.jvector.graph.similarity.ScoreFunction;
import io.github.jbellis.jvector.pq.LocallyAdaptiveVectorQuantization;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.types.VectorFloat;

import java.io.DataOutput;
import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * Implements the storage of LVQ-quantized vectors in an on-disk graph index. These can be used for reranking.
 */
public class LVQ implements Feature {
    private final LocallyAdaptiveVectorQuantization lvq;

    public LVQ(LocallyAdaptiveVectorQuantization lvq) {
        this.lvq = lvq;
    }

    @Override
    public FeatureId id() {
        return FeatureId.LVQ;
    }

    @Override
    public int headerSize() {
        return lvq.compressorSize();
    }

    @Override
    public int inlineSize() {
        return lvq.compressedVectorSize();
    }

    public int dimension() {
        return lvq.globalMean.length();
    }

    static LVQ load(CommonHeader header, RandomAccessReader reader) {
        try {
            return new LVQ(LocallyAdaptiveVectorQuantization.load(reader));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void writeHeader(DataOutput out) throws IOException {
        lvq.write(out);
    }

    @Override
    public void writeInline(DataOutput out, Feature.State state_) throws IOException {
        var state = (LVQ.State) state_;
        state.vector.writePacked(out);
    }

    public static class State implements Feature.State {
        public final LocallyAdaptiveVectorQuantization.QuantizedVector vector;

        public State(LocallyAdaptiveVectorQuantization.QuantizedVector vector) {
            this.vector = vector;
        }
    }

    ScoreFunction.ExactScoreFunction rerankerFor(VectorFloat<?> queryVector,
                                                 VectorSimilarityFunction vsf,
                                                 OnDiskGraphIndex.View view)
    {
        return lvq.scoreFunctionFrom(queryVector, vsf, new PackedVectors(view));
    }

    private class PackedVectors implements LVQPackedVectors {
        private final OnDiskGraphIndex.View view;

        public PackedVectors(OnDiskGraphIndex.View view) {
            this.view = view;
        }

        @Override
        public LocallyAdaptiveVectorQuantization.PackedVector getPackedVector(int ordinal) {
            var reader = view.inlineReaderForNode(ordinal, FeatureId.LVQ);
            try {
                var bias = reader.readFloat();
                var scale = reader.readFloat();
                // reduce the size by 2 floats read as bias/scale
                var packed = OnDiskGraphIndex.vectorTypeSupport.readByteSequence(reader, lvq.compressedVectorSize() - 2 * Float.BYTES);
                return new LocallyAdaptiveVectorQuantization.PackedVector(packed, bias, scale);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
