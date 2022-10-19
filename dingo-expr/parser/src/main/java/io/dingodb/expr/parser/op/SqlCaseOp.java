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
import io.dingodb.expr.runtime.op.logical.RtLogicalOp;
import io.dingodb.expr.runtime.op.sql.RtSqlCaseOp;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.ArrayList;
import java.util.List;

public final class SqlCaseOp extends OpIgnoreEnv {
    public static final String FUN_NAME = "CASE";

    private SqlCaseOp() {
        super(FUN_NAME);
    }

    public static @NonNull SqlCaseOp fun() {
        return new SqlCaseOp();
    }

    @Override
    protected @NonNull RtExpr evalNullConst(RtExpr @NonNull [] rtExprArray) throws DingoExprCompileException {
        int size = rtExprArray.length;
        List<RtExpr> nonConstExprs = new ArrayList<>(size);
        try {
            for (int i = 0; i < size - 1; i += 2) {
                RtExpr rtExpr = rtExprArray[i];
                if (rtExpr instanceof RtNull) {
                    continue;
                }
                if (rtExpr instanceof RtConst) {
                    Object v = rtExpr.eval(null);
                    if (RtLogicalOp.test(v)) {
                        return rtExprArray[i + 1];
                    }
                } else {
                    nonConstExprs.add(rtExpr);
                    nonConstExprs.add(rtExprArray[i + 1]);
                }
            }
            if (nonConstExprs.size() == 0) {
                return rtExprArray[size - 1];
            }
            nonConstExprs.add(rtExprArray[size - 1]);
            return createRtOp(nonConstExprs.toArray(new RtExpr[0]));
        } catch (FailGetEvaluator e) {
            throw new DingoExprCompileException(e);
        }
    }

    @Override
    protected @NonNull RtSqlCaseOp createRtOp(RtExpr[] rtExprArray) throws FailGetEvaluator {
        return new RtSqlCaseOp(rtExprArray);
    }
}
