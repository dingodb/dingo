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

package io.dingodb.expr.runtime.op;

import io.dingodb.expr.runtime.EvalContext;
import io.dingodb.expr.runtime.EvalEnv;
import io.dingodb.expr.runtime.RtExpr;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

public abstract class RtEnvFun extends RtOp {
    private static final long serialVersionUID = -6708361856536230351L;

    protected RtEnvFun(RtExpr[] paras) {
        super(paras);
    }

    protected abstract @Nullable Object envFun(
        @NonNull Object @NonNull [] values,
        @Nullable EvalEnv env
    );

    @Override
    public @Nullable Object eval(EvalContext etx) {
        Object[] paraValues = new Object[paras.length];
        int i = 0;
        for (RtExpr para : paras) {
            Object v = para.eval(etx);
            if (v == null) {
                return null;
            }
            paraValues[i++] = v;
        }
        return envFun(paraValues, etx != null ? etx.getEnv() : null);
    }
}
