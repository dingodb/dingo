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

package io.dingodb.expr.parser.op;

import io.dingodb.expr.parser.exception.DingoExprCompileException;
import io.dingodb.expr.parser.exception.InvalidIndex;
import io.dingodb.expr.parser.var.VarStub;
import io.dingodb.expr.runtime.CompileContext;
import io.dingodb.expr.runtime.RtConst;
import io.dingodb.expr.runtime.RtExpr;
import io.dingodb.expr.runtime.evaluator.index.IndexEvaluatorFactory;
import io.dingodb.expr.runtime.exception.FailGetEvaluator;
import io.dingodb.expr.runtime.op.RtOp;

import javax.annotation.Nonnull;

public final class IndexOp extends OpWithEvaluator {
    /**
     * Create an IndexOp. IndexOp can get an element of a Map or an Array.
     */
    public IndexOp() {
        super(OpType.INDEX, IndexEvaluatorFactory.INSTANCE);
    }

    @Nonnull
    @Override
    public RtExpr compileIn(CompileContext ctx) throws DingoExprCompileException {
        RtExpr[] rtExprArray = compileExprArray(ctx);
        if (rtExprArray[0] instanceof VarStub) {
            VarStub stub = (VarStub) rtExprArray[0];
            if (rtExprArray[1] instanceof RtConst) {
                Object index = ((RtConst) rtExprArray[1]).eval(null);
                if (index instanceof Number) {
                    return stub.getElement(((Number) index).intValue());
                } else if (index instanceof String) {
                    return stub.getElement(index);
                }
            }
            throw new InvalidIndex(rtExprArray[1]);
        }
        return evalNullConst(rtExprArray);
    }

    @Override
    protected RtOp createRtOp(RtExpr[] rtExprArray) throws FailGetEvaluator {
        return super.createRtOp(rtExprArray);
    }
}
