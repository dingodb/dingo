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
import io.dingodb.expr.runtime.op.BinaryOp;
import io.dingodb.expr.runtime.op.OpKey;
import io.dingodb.expr.runtime.type.Type;
import io.dingodb.expr.runtime.type.Types;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Arrays;

public class VectorTextFun extends BinaryOp {
    public static final VectorTextFun INSTANCE = new VectorTextFun();

    public static final String NAME = "txt2vec";

    private static final long serialVersionUID = 8043535092918925720L;

    private VectorTextFun() {
    }

    @Override
    public Type getType() {
        return Types.LIST_FLOAT;
    }

    @Override
    public @NonNull String getName() {
        return NAME;
    }

    @Override
    protected Object evalNonNullValue(@NonNull Object value0, @NonNull Object value1, ExprConfig config) {
        Float[] vector = VectorExtract.getTxtVector(VectorTextFun.NAME, (String) value0, value1);
        return Arrays.asList(vector);
    }

    @Override
    public OpKey keyOf(@NonNull Type type0, @NonNull Type type1) {
        if (type0.equals(Types.STRING) && type1.equals(Types.STRING)) {
            return Types.STRING;
        }
        return null;
    }

    @Override
    public BinaryOp getOp(OpKey key) {
        return (key != null && key.equals(Types.STRING)) ? INSTANCE : null;
    }
}
