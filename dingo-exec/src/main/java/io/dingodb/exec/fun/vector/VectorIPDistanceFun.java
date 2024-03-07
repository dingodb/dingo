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

import io.dingodb.common.exception.DingoSqlException;
import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.expr.runtime.op.BinaryOp;
import io.dingodb.expr.runtime.op.OpKey;
import io.dingodb.expr.runtime.type.Type;
import io.dingodb.expr.runtime.type.Types;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class VectorIPDistanceFun extends BinaryVectorVectorFun {
    public static final VectorIPDistanceFun INSTANCE = new VectorIPDistanceFun();
    public static final String NAME = "IPDistance";

    private static final long serialVersionUID = 7869256649847747534L;

    public static double innerProduct(List<Float> vectorA, List<Float> vectorB) {
        double dotProduct = 0.0;
        for (int i = 0; i < vectorA.size(); i++) {
            dotProduct += vectorA.get(i) * vectorB.get(i);
        }
        return 1 - dotProduct;
    }

    public static double innerProductCombine(List<Float> vectorA, List<Number> vectorB) {
        if (vectorA != null && vectorA.size() != vectorB.size()) {
            throw new DingoSqlException(
                "The dimensions of the source vector and the target vector must be consistent",5001, "45000"
            );
        }
        double dotProduct = 0.0;
        for (int i = 0; i < vectorA.size(); i++) {
            dotProduct += vectorA.get(i) * vectorB.get(i).floatValue();
        }
        return 1 - dotProduct;
    }

    @Override
    protected Object evalNonNullValue(@NonNull Object value0, @NonNull Object value1, ExprConfig config) {
        double distance = innerProductCombine((List<Float>) value0, (List<Number>) value1);
        BigDecimal distanceAccurate = new BigDecimal(distance);
        return distanceAccurate.floatValue();
    }

    @Override
    public @NonNull String getName() {
        return NAME;
    }

    @Override
    public Type getType() {
        return Types.LIST_FLOAT;
    }

    @Override
    public BinaryOp getOp(OpKey key) {
        return INSTANCE;
    }
}
