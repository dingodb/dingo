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

import io.dingodb.expr.runtime.op.BinaryOp;
import io.dingodb.expr.runtime.op.OpKey;
import io.dingodb.expr.runtime.type.Type;
import io.dingodb.expr.runtime.type.Types;
import org.checkerframework.checker.nullness.qual.NonNull;

public abstract class BinaryVectorVectorFun extends BinaryOp {
    private static final long serialVersionUID = 9015040211359619477L;

    @Override
    public OpKey keyOf(@NonNull Type type0, @NonNull Type type1) {
        if (type0.equals((Types.LIST_FLOAT)) && type1.equals((Types.LIST_FLOAT))) {
            return Types.LIST_FLOAT;
        }
        return null;
    }

    @Override
    public BinaryOp getOp(OpKey key) {
        return (key != null && key.equals(Types.LIST_FLOAT)) ? this : null;
    }
}
