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

import java.util.List;

public class VectorL2DistanceFun extends RtFun {
    public static final String NAME = "l2Distance";

    private static final long serialVersionUID = -8016553875879480268L;

    public VectorL2DistanceFun(@NonNull RtExpr[] paras) {
        super(paras);
    }


    @Override
    public int typeCode() {
        return TypeCode.ARRAY;
    }

    @Override
    protected @Nullable Object fun(@NonNull Object @NonNull [] values) {
        Double distance = l2Distance((List<Float>) values[0], (List<Float>) values[1]);
        return distance.floatValue();
    }

    private double l2Distance(List<Float> vectorA, List<Float> vectorB) {
        double distance = 0.0;
        for (int i = 0; i < vectorA.size(); i++) {
            distance += Math.pow(vectorA.get(i) - vectorB.get(i), 2);
        }
        return Math.sqrt(distance);
    }
}
