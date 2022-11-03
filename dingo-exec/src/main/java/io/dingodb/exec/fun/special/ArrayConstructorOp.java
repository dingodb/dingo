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

import io.dingodb.exec.exception.ElementsNullNotAllowed;
import io.dingodb.expr.core.TypeCode;
import io.dingodb.expr.parser.op.Op;
import io.dingodb.expr.runtime.EvalContext;
import io.dingodb.expr.runtime.RtExpr;
import io.dingodb.expr.runtime.op.RtOp;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class ArrayConstructorOp extends Op {
    public static final String NAME = "ARRAY";

    private ArrayConstructorOp() {
        super(NAME);
    }

    public static @NonNull ArrayConstructorOp fun() {
        return new ArrayConstructorOp();
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
        private static final long serialVersionUID = -2386239654203654097L;

        public Runtime(RtExpr[] paras) {
            super(paras);
        }

        @Override
        public @NonNull Object eval(EvalContext etx) {
            int size = paras.length;
            Object[] result = new Object[size];
            for (int i = 0; i < size; ++i) {
                RtExpr para = paras[i];
                Object v = para.eval(etx);
                if (v == null) {
                    throw new ElementsNullNotAllowed();
                }
                result[i] = v;
            }
            return result;
        }

        @Override
        public int typeCode() {
            return TypeCode.ARRAY;
        }
    }
}
