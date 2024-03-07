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
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;

public class VectorL2DistanceFun extends BinaryVectorVectorFun {
    public static final VectorL2DistanceFun INSTANCE = new VectorL2DistanceFun();

    public static final String NAME = "l2Distance";

    private static final long serialVersionUID = -8016553875879480268L;

    private VectorL2DistanceFun() {
    }

    public static double l2DistanceCombine(@NonNull List<Float> vectorA, List<Number> vectorB) {
        if (vectorA.size() != vectorB.size()) {
            throw new DingoSqlException(
                "The dimensions of the source vector and the target vector must be consistent",5001, "45000"
            );
        }
        double distance = 0.0;
        for (int i = 0; i < vectorA.size(); i++) {
            distance += Math.pow(vectorA.get(i) - vectorB.get(i).floatValue(), 2);
        }
        return distance;
    }

    @Override
    protected Object evalNonNullValue(@NonNull Object value0, @NonNull Object value1, ExprConfig config) {
        double distance = l2DistanceCombine((List<Float>) value0, (List<Number>) value1);
        return (float) distance;
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
