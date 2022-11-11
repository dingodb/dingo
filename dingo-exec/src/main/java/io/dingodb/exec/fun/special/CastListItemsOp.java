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
import io.dingodb.expr.core.evaluator.Evaluator;
import io.dingodb.expr.core.evaluator.EvaluatorKey;
import io.dingodb.expr.parser.DefaultFunFactory;
import io.dingodb.expr.parser.op.Op;
import io.dingodb.expr.runtime.RtExpr;
import io.dingodb.expr.runtime.op.RtFun;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class CastListItemsOp extends Op {
    public static final String NAME = "CAST_LIST_ITEMS";

    private CastListItemsOp() {
        super(NAME);
    }

    public static @NonNull CastListItemsOp fun() {
        return new CastListItemsOp();
    }

    @Override
    protected @NonNull Runtime createRtOp(RtExpr @NonNull [] rtExprArray) {
        int toTypeCode = TypeCode.codeOf((String) Objects.requireNonNull(rtExprArray[0].eval(null)));
        // The types of array elements cannot be achieved, even not the same, so cast with universal evaluators.
        return new Runtime(
            DefaultFunFactory.getCastEvaluatorFactory(toTypeCode).getEvaluator(EvaluatorKey.UNIVERSAL),
            new RtExpr[]{rtExprArray[1]}
        );
    }

    public static final class Runtime extends RtFun {
        private static final long serialVersionUID = -7968394097724524958L;

        private final @NonNull Evaluator evaluator;

        public Runtime(@NonNull Evaluator evaluator, RtExpr[] paras) {
            super(paras);
            this.evaluator = evaluator;
        }

        @Override
        protected @NonNull Object fun(@NonNull Object @NonNull [] values) {
            List<?> list = (List<?>) values[0];
            List<Object> result = new ArrayList<>(list.size());
            for (Object i : list) {
                Object v = evaluator.eval(new Object[]{i});
                if (v != null) {
                    result.add(v);
                } else {
                    throw new ElementsNullNotAllowed();
                }
            }
            return result;
        }

        @Override
        public int typeCode() {
            return TypeCode.LIST;
        }
    }
}
