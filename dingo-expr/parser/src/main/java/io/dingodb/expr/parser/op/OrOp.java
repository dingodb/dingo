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
import io.dingodb.expr.runtime.RtConst;
import io.dingodb.expr.runtime.RtExpr;
import io.dingodb.expr.runtime.RtNull;
import io.dingodb.expr.runtime.exception.FailGetEvaluator;
import io.dingodb.expr.runtime.op.RtOp;
import io.dingodb.expr.runtime.op.logical.RtLogicalOp;
import io.dingodb.expr.runtime.op.logical.RtOrOp;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;

public final class OrOp extends Op {
    public static final String FUN_NAME = "OR";

    private OrOp(OpType type) {
        super(type);
        name = FUN_NAME;
    }

    @Nonnull
    public static OrOp op() {
        return new OrOp(OpType.OR);
    }

    @Nonnull
    public static OrOp fun() {
        return new OrOp(OpType.FUN);
    }

    @Nonnull
    @Override
    protected RtExpr evalNullConst(@Nonnull RtExpr[] rtExprArray) throws DingoExprCompileException {
        int size = rtExprArray.length;
        try {
            List<RtExpr> nonConstExprs = new ArrayList<>(size);
            RtExpr result = RtConst.FALSE;
            for (RtExpr rtExpr : rtExprArray) {
                if (rtExpr instanceof RtConst || rtExpr instanceof RtNull) {
                    Object v = rtExpr.eval(null);
                    if (v == null) {
                        if (result == RtConst.FALSE) {
                            result = RtNull.INSTANCE;
                        }
                    } else if (RtLogicalOp.test(v)) {
                        return RtConst.TRUE;
                    }
                } else {
                    nonConstExprs.add(rtExpr);
                }
            }
            if (nonConstExprs.size() == 0) {
                return result;
            } else {
                if (result == RtNull.INSTANCE) {
                    nonConstExprs.add(result);
                }
                return createRtOp(nonConstExprs.toArray(new RtExpr[0]));
            }
        } catch (FailGetEvaluator e) {
            throw new DingoExprCompileException(e);
        }
    }

    @Nonnull
    @Override
    protected RtOp createRtOp(RtExpr[] rtExprArray) throws FailGetEvaluator {
        return new RtOrOp(rtExprArray);
    }
}
