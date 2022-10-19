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
import io.dingodb.expr.runtime.EvalEnv;
import io.dingodb.expr.runtime.RtExpr;
import org.checkerframework.checker.nullness.qual.NonNull;

public abstract class OpIgnoreEnv extends Op {
    protected OpIgnoreEnv(OpType type) {
        super(type);
    }

    protected OpIgnoreEnv(String name) {
        super(name);
    }

    protected abstract @NonNull RtExpr evalNullConst(RtExpr[] rtExprArray) throws DingoExprCompileException;

    @Override
    protected @NonNull RtExpr evalNullConstEnv(
        RtExpr[] rtExprArray,
        EvalEnv env
    ) throws DingoExprCompileException {
        return evalNullConst(rtExprArray);
    }
}
