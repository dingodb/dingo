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
import io.dingodb.expr.parser.ExprVisitor;
import io.dingodb.expr.parser.exception.ElementNotExists;
import io.dingodb.expr.runtime.CompileContext;
import io.dingodb.expr.runtime.RtConst;
import io.dingodb.expr.runtime.RtExpr;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class Var implements Expr {
    public static final String WHOLE_VAR_NAME = "$";

    public static final Var WHOLE = new Var(WHOLE_VAR_NAME);

    @Getter
    @EqualsAndHashCode.Include
    private final String name;

    public static Var of(@NonNull String name) {
        if (name.equals(WHOLE_VAR_NAME)) {
            return WHOLE;
        }
        return new Var(name);
    }

    static @NonNull RtExpr createVar(@NonNull CompileContext ctx) {
        RtExpr rtExpr = ctx.createVar();
        return rtExpr != null ? rtExpr : new VarStub(ctx);
    }

    @Override
    public @NonNull RtExpr compileIn(@Nullable CompileContext ctx) throws ElementNotExists {
        RtConst rtConst = ConstFactory.INS.getConst(name);
        if (rtConst != null) {
            return rtConst;
        }
        if (ctx != null) {
            if (name.equals(WHOLE_VAR_NAME)) {
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
    public <T> T accept(@NonNull ExprVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String toString() {
        return name;
    }
}
