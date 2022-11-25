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

package io.dingodb.exec.fun.special;

import io.dingodb.expr.core.TypeCode;
import io.dingodb.expr.parser.op.Op;
import io.dingodb.expr.runtime.EvalContext;
import io.dingodb.expr.runtime.RtExpr;
import io.dingodb.expr.runtime.op.RtOp;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.HashMap;
import java.util.Map;

public final class MapConstructorOp extends Op {
    public static final String NAME = "MAP";

    private MapConstructorOp() {
        super(NAME);
    }

    public static @NonNull MapConstructorOp fun() {
        return new MapConstructorOp();
    }

    @Override
    protected boolean evalNull(RtExpr @NonNull [] rtExprArray) {
        checkNoNulls(rtExprArray);
        return false;
    }

    @Override
    protected @NonNull Runtime createRtOp(RtExpr[] rtExprArray) {
        return new Runtime(rtExprArray);
    }

    public static final class Runtime extends RtOp {
        private static final long serialVersionUID = 1747193458887448932L;

        public Runtime(RtExpr[] paras) {
            super(paras);
        }

        @Override
        public @NonNull Object eval(EvalContext etx) {
            int size = paras.length;
            Map<Object, Object> result = new HashMap<>();
            for (int i = 0; i < size; i += 2) {
                result.put(paras[i].eval(etx), paras[i + 1].eval(etx));
            }
            return result;
        }

        @Override
        public int typeCode() {
            return TypeCode.MAP;
        }
    }
}
