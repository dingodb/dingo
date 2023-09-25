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

import io.dingodb.expr.core.TypeCode;
import io.dingodb.expr.runtime.RtExpr;
import io.dingodb.expr.runtime.op.RtFun;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;

import static io.dingodb.exec.fun.vector.VectorIPDistanceFun.innerProduct;

public class VectorCosineDistanceFun extends RtFun {
    private static final long serialVersionUID = 7709745346405714020L;
    public static final String NAME = "cosineDistance";

    public VectorCosineDistanceFun(@NonNull RtExpr[] paras) {
        super(paras);
    }

    @Override
    public int typeCode() {
        return TypeCode.ARRAY;
    }

    @Override
    protected @Nullable Object fun(@NonNull Object @NonNull [] values) {
        List<Float> vectorA = transform((List<Float>) values[0]);
        List<Float> vectorB = transform((List<Float>) values[1]);
        Double distance = innerProduct(vectorA, vectorB);
        return distance.floatValue();
    }

    private static final double tmp = 1E-30;
    private static List<Float> transform(List<Float> vector) {
        int dimension = vector.size();
        List<Float> vectorRes = new ArrayList<>(dimension);
        double norm = 0.0f;
        for (int i = 0; i < dimension; i++) {
            norm += vector.get(i) * vector.get(i);
        }
        norm = 1.0 / (Math.sqrt(norm) + tmp);
        for (int i = 0; i < dimension; i ++) {
            vectorRes.add((float) (vector.get(i) * norm));
        }
        return vectorRes;
    }

}
