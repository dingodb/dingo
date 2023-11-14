/*
 * Copyright 2021 DataCanvas
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.dingodb.exec.fun.vector;

import io.dingodb.expr.runtime.ExprConfig;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;

import static io.dingodb.exec.fun.vector.VectorIPDistanceFun.innerProduct;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class VectorCosineDistanceFun extends BinaryVectorVectorFun {
    public static final VectorCosineDistanceFun INSTANCE = new VectorCosineDistanceFun();
    public static final String NAME = "cosineDistance";

    private static final long serialVersionUID = 7709745346405714020L;

    private static final double tmp = 1E-30;

    private static List<Float> transform(List<Float> vector) {
        int dimension = vector.size();
        List<Float> vectorRes = new ArrayList<>(dimension);
        double norm = 0.0f;
        for (int i = 0; i < dimension; i++) {
            norm += vector.get(i) * vector.get(i);
        }
        norm = 1.0 / (Math.sqrt(norm) + tmp);
        for (int i = 0; i < dimension; i++) {
            vectorRes.add((float) (vector.get(i) * norm));
        }
        return vectorRes;
    }

    @Override
    protected Object evalNonNullValue(@NonNull Object value0, @NonNull Object value1, ExprConfig config) {
        List<Float> vectorA = transform((List<Float>) value0);
        List<Float> vectorB = transform((List<Float>) value1);
        double distance = innerProduct(vectorA, vectorB);
        return (float) distance;
    }
}
