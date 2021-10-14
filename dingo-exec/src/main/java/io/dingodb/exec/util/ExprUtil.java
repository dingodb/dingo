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

package io.dingodb.exec.util;

import io.dingodb.expr.parser.exception.DingoExprCompileException;
import io.dingodb.expr.parser.exception.DingoExprParseException;
import io.dingodb.expr.parser.parser.DingoExprCompiler;
import io.dingodb.expr.runtime.CompileContext;
import io.dingodb.expr.runtime.RtExpr;

import java.util.List;
import javax.annotation.Nonnull;

public final class ExprUtil {
    private ExprUtil() {
    }

    @Nonnull
    public static RtExpr compileExpr(String expr, @Nonnull CompileContext ctx) {
        try {
            return DingoExprCompiler.parse(expr).compileIn(ctx);
        } catch (DingoExprParseException | DingoExprCompileException e) {
            throw new IllegalStateException(e);
        }
    }

    @Nonnull
    public static RtExpr[] compileExprList(@Nonnull List<String> exprList, @Nonnull CompileContext ctx) {
        return exprList.stream()
            .map(e -> compileExpr(e, ctx))
            .toArray(RtExpr[]::new);
    }
}
