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
import io.dingodb.expr.runtime.op.BinaryOp;
import io.dingodb.expr.runtime.op.OpKey;
import io.dingodb.expr.runtime.type.Type;
import io.dingodb.expr.runtime.type.Types;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.checkerframework.checker.nullness.qual.NonNull;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class VectorDistanceFun extends BinaryVectorVectorFun {
    public static final VectorDistanceFun INSTANCE = new VectorDistanceFun();
    public static final String NAME = "distance";
    private static final long serialVersionUID = -8016553875879480268L;

    @Override
    public Object evalValue(Object value0, Object value1, ExprConfig config) {
        return null;
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
