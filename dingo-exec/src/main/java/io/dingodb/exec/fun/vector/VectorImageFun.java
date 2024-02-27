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
import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.expr.runtime.op.OpKey;
import io.dingodb.expr.runtime.op.TertiaryOp;
import io.dingodb.expr.runtime.type.Type;
import io.dingodb.expr.runtime.type.Types;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Arrays;

public class VectorImageFun extends TertiaryOp {
    public static final VectorImageFun INSTANCE = new VectorImageFun();

    public static final String NAME = "img2vec";

    private static final long serialVersionUID = 2796411347891525563L;

    private VectorImageFun() {
    }

    @Override
    public Type getType() {
        return Types.LIST_FLOAT;
    }

    @Override
    protected Object evalNonNullValue(
        @NonNull Object value0,
        @NonNull Object value1,
        @NonNull Object value2,
        ExprConfig config
    ) {
        Float[] vector = VectorExtract.getImgVector(VectorImageFun.NAME, (String) value0, value1, (Boolean) value2);
        return Arrays.asList(vector);
    }

    @Override
    public @NonNull String getName() {
        return NAME;
    }

    @Override
    public OpKey keyOf(@NonNull Type type0, @NonNull Type type1, @NonNull Type type2) {
        if (type0.equals(Types.STRING) && type1.equals(Types.STRING) && type2.equals(Types.BOOL)) {
            return Types.STRING;
        }
        return null;
    }

    @Override
    public TertiaryOp getOp(OpKey key) {
        return (key != null && key.equals(Types.STRING)) ? INSTANCE : null;
    }
}
