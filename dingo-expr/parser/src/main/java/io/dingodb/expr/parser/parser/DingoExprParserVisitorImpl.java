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

package io.dingodb.expr.parser.parser;

import io.dingodb.expr.parser.DingoExprParser;
import io.dingodb.expr.parser.DingoExprParserBaseVisitor;
import io.dingodb.expr.parser.Expr;
import io.dingodb.expr.parser.op.FunFactory;
import io.dingodb.expr.parser.op.IndexOp;
import io.dingodb.expr.parser.op.Op;
import io.dingodb.expr.parser.op.OpFactory;
import io.dingodb.expr.parser.value.Null;
import io.dingodb.expr.parser.value.Value;
import io.dingodb.expr.parser.var.Var;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;

final class DingoExprParserVisitorImpl extends DingoExprParserBaseVisitor<Expr> {
    private final boolean realAsBigDecimal;

    public DingoExprParserVisitorImpl(boolean realAsBigDecimal) {
        this.realAsBigDecimal = realAsBigDecimal;
    }

    private void setParaList(@NonNull Op op, @NonNull List<DingoExprParser.ExprContext> exprList) {
        op.setExprArray(
            exprList.stream()
                .map(this::visit)
                .toArray(Expr[]::new)
        );
    }

    private @NonNull Expr internalVisitBinaryOp(
        int type,
        List<DingoExprParser.ExprContext> exprList
    ) {
        Op op = OpFactory.getBinary(type);
        setParaList(op, exprList);
        return op;
    }

    private @NonNull Expr internalVisitUnaryOp(
        int type,
        DingoExprParser.ExprContext expr
    ) {
        Op op = OpFactory.getUnary(type);
        op.setExprArray(new Expr[]{visit(expr)});
        return op;
    }

    @Override
    public @NonNull Expr visitOr(DingoExprParser.@NonNull OrContext ctx) {
        return internalVisitBinaryOp(ctx.op.getType(), ctx.expr());
    }

    @Override
    public @NonNull Expr visitMulDiv(DingoExprParser.@NonNull MulDivContext ctx) {
        return internalVisitBinaryOp(ctx.op.getType(), ctx.expr());
    }

    @Override
    public @NonNull Expr visitAddSub(DingoExprParser.@NonNull AddSubContext ctx) {
        return internalVisitBinaryOp(ctx.op.getType(), ctx.expr());
    }

    @Override
    public Expr visitVar(DingoExprParser.@NonNull VarContext ctx) {
        String name = ctx.ID().getText();
        if (name.equals("null") || name.equals("NULL")) {
            return Null.INSTANCE;
        }
        return new Var(ctx.ID().getText());
    }

    @Override
    public @NonNull Expr visitPosNeg(DingoExprParser.@NonNull PosNegContext ctx) {
        return internalVisitUnaryOp(ctx.op.getType(), ctx.expr());
    }

    @Override
    public @NonNull Expr visitStringOp(DingoExprParser.@NonNull StringOpContext ctx) {
        return internalVisitBinaryOp(ctx.op.getType(), ctx.expr());
    }

    @Override
    public @NonNull Expr visitIndex(DingoExprParser.@NonNull IndexContext ctx) {
        Op op = new IndexOp();
        setParaList(op, ctx.expr());
        return op;
    }

    @Override
    public @NonNull Expr visitInt(DingoExprParser.@NonNull IntContext ctx) {
        try {
            return Value.parseLong(ctx.INT().getText());
        } catch (NumberFormatException e) { // overflow
            return Value.parseDecimal(ctx.INT().getText());
        }
    }

    @Override
    public @NonNull Expr visitStr(DingoExprParser.@NonNull StrContext ctx) {
        return Value.parseString(ctx.STR().getText());
    }

    @Override
    public @NonNull Expr visitNot(DingoExprParser.@NonNull NotContext ctx) {
        return internalVisitUnaryOp(ctx.op.getType(), ctx.expr());
    }

    @Override
    public @NonNull Expr visitRelation(DingoExprParser.@NonNull RelationContext ctx) {
        return internalVisitBinaryOp(ctx.op.getType(), ctx.expr());
    }

    @Override
    public @NonNull Expr visitStrIndex(DingoExprParser.@NonNull StrIndexContext ctx) {
        Op op = new IndexOp();
        op.setExprArray(new Expr[]{visit(ctx.expr()), Value.of(ctx.ID().getText())});
        return op;
    }

    @Override
    public @NonNull Expr visitBool(DingoExprParser.@NonNull BoolContext ctx) {
        return Value.parseBoolean(ctx.BOOL().getText());
    }

    @Override
    public @NonNull Expr visitAnd(DingoExprParser.@NonNull AndContext ctx) {
        return internalVisitBinaryOp(ctx.op.getType(), ctx.expr());
    }

    @Override
    public @NonNull Expr visitPars(DingoExprParser.@NonNull ParsContext ctx) {
        return visit(ctx.expr());
    }

    @Override
    public @NonNull Expr visitReal(DingoExprParser.@NonNull RealContext ctx) {
        if (realAsBigDecimal) {
            return Value.parseDecimal(ctx.REAL().getText());
        } else {
            return Value.parseDouble(ctx.REAL().getText());
        }
    }

    @Override
    public @NonNull Expr visitFun(DingoExprParser.@NonNull FunContext ctx) {
        String funName = ctx.ID().getText();
        Op op = FunFactory.INS.getFun(funName);
        setParaList(op, ctx.expr());
        return op;
    }
}
