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

package io.dingodb.expr.parser.value;

import io.dingodb.expr.parser.Expr;
import io.dingodb.expr.parser.exception.DingoExprCompileException;
import io.dingodb.expr.runtime.CompileContext;
import io.dingodb.expr.runtime.RtExpr;
import io.dingodb.expr.runtime.RtNull;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class Null implements Expr {
    public static final Null INSTANCE = new Null();

    private Null() {
    }

    @Override
    public @NonNull RtExpr compileIn(CompileContext ctx) throws DingoExprCompileException {
        return RtNull.INSTANCE;
    }

    @Override
    public @NonNull String toString() {
        return "NULL";
    }
}
