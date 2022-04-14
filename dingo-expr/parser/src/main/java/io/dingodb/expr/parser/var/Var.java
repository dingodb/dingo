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

package io.dingodb.expr.parser.var;

import io.dingodb.expr.parser.Expr;
import io.dingodb.expr.parser.exception.ElementNotExists;
import io.dingodb.expr.runtime.CompileContext;
import io.dingodb.expr.runtime.RtConst;
import io.dingodb.expr.runtime.RtExpr;
import lombok.RequiredArgsConstructor;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

@RequiredArgsConstructor
public class Var implements Expr {
    private final String name;

    @Nonnull
    static RtExpr createVar(@Nonnull CompileContext ctx) {
        RtExpr rtExpr = ctx.createVar();
        return rtExpr != null ? rtExpr : new VarStub(ctx);
    }

    @Nonnull
    @Override
    public RtExpr compileIn(@Nullable CompileContext ctx) throws ElementNotExists {
        RtConst rtConst = ConstFactory.INS.getConst(name);
        if (rtConst != null) {
            return rtConst;
        }
        if (ctx != null) {
            if (name.equals("$")) {
                return createVar(ctx);
            }
            CompileContext child = ctx.getChild(name);
            if (child != null) {
                return createVar(child);
            }
        }
        throw new ElementNotExists(name, ctx);
    }

    @Override
    public String toString() {
        return name;
    }
}
