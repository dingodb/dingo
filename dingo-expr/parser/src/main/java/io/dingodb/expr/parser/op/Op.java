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

import io.dingodb.expr.parser.Expr;
import io.dingodb.expr.parser.exception.DingoExprCompileException;
import io.dingodb.expr.runtime.CompileContext;
import io.dingodb.expr.runtime.RtConst;
import io.dingodb.expr.runtime.RtExpr;
import io.dingodb.expr.runtime.RtNull;
import io.dingodb.expr.runtime.exception.FailGetEvaluator;
import io.dingodb.expr.runtime.op.RtOp;
import lombok.Getter;
import lombok.Setter;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public abstract class Op implements Expr {
    @Getter
    protected OpType type;
    protected String name;
    @Setter
    protected Expr[] exprArray;

    protected Op(OpType type) {
        this.type = type;
        this.name = null;
    }

    protected Op(String name) {
        this.type = OpType.FUN;
        this.name = name;
    }

    public String getName() {
        return type == OpType.FUN ? name : type.getSymbol();
    }

    public int getPrecedence() {
        return type.getPrecedence();
    }

    @Nonnull
    protected final RtExpr[] compileExprArray(CompileContext ctx) throws DingoExprCompileException {
        RtExpr[] rtExprArray = new RtExpr[exprArray.length];
        int i = 0;
        for (Expr expr : exprArray) {
            rtExprArray[i++] = expr.compileIn(ctx);
        }
        return rtExprArray;
    }

    @Nonnull
    protected RtExpr evalNullConst(@Nonnull RtExpr[] rtExprArray) throws DingoExprCompileException {
        try {
            if (Arrays.stream(rtExprArray).anyMatch(e -> e instanceof RtNull)) {
                return RtNull.INSTANCE;
            }
            RtOp rtOp = createRtOp(rtExprArray);
            if (Arrays.stream(rtExprArray).allMatch(e -> e instanceof RtConst)) {
                return new RtConst(rtOp.eval(null));
            }
            return rtOp;
        } catch (FailGetEvaluator e) {
            throw new DingoExprCompileException(e);
        }
    }

    @Nonnull
    @Override
    public RtExpr compileIn(CompileContext ctx) throws DingoExprCompileException {
        RtExpr[] rtExprArray = compileExprArray(ctx);
        return evalNullConst(rtExprArray);
    }

    protected abstract RtOp createRtOp(RtExpr[] rtExprArray) throws FailGetEvaluator;

    @Override
    public String toString() {
        List<String> subs = Arrays.stream(exprArray)
            .map(e -> {
                if (getType() != OpType.INDEX && getType() != OpType.FUN && e instanceof Op) {
                    Op op = (Op) e;
                    if (op.getPrecedence() > getPrecedence()) {
                        return "(" + e + ")";
                    }
                }
                return e.toString();
            })
            .collect(Collectors.toList());
        if (type == OpType.INDEX) {
            assertOperandsSize(2);
            return subs.get(0) + "[" + subs.get(1) + "]";
        } else if (type != OpType.FUN) {
            switch (type.getCategory()) {
                case UNARY:
                    assertOperandsSize(1);
                    return getName() + subs.get(0);
                case BINARY:
                    assertOperandsSize(2);
                    return subs.get(0) + getName() + subs.get(1);
                default:
                    break;
            }
        }
        return getName() + "(" + String.join(", ", subs) + ")";
    }

    private void assertOperandsSize(int num) {
        assert exprArray.length == num : "\"" + getName() + "\" operation requires " + num + " operand(s).";
    }
}
