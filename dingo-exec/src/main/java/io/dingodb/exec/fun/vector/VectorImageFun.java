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

import io.dingodb.exec.restful.VectorExtract;
import io.dingodb.expr.core.TypeCode;
import io.dingodb.expr.runtime.RtExpr;
import io.dingodb.expr.runtime.op.RtFun;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;

public class VectorImageFun extends RtFun {
    public static final String NAME = "img2vec";

    private static final long serialVersionUID = 2796411347891525563L;

    public VectorImageFun(@NonNull RtExpr[] paras) {
        super(paras);
    }


    @Override
    public int typeCode() {
        return TypeCode.ARRAY;
    }

    @Override
    protected @Nullable Object fun(@NonNull Object @NonNull [] values) {
        if (values.length < 3) {
            throw new RuntimeException("vector load param error");
        }
        if (!(values[0] instanceof String) || !(values[1] instanceof String) || !(values[2] instanceof Boolean)) {
            throw new RuntimeException("vector load param error");
        }
        Float[] vector = VectorExtract.getImgVector(VectorImageFun.NAME, (String) values[0],
            values[1], (Boolean) values[2]);
        return Arrays.asList(vector);
    }
}
