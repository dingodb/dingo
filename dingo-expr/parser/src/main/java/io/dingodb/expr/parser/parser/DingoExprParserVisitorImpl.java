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

import java.util.List;
import javax.annotation.Nonnull;

public final class DingoExprParserVisitorImpl extends DingoExprParserBaseVisitor<Expr> {
    private void setParaList(@Nonnull Op op, @Nonnull List<DingoExprParser.ExprContext> exprList) {
        op.setExprArray(
            exprList.stream()
                .map(this::visit)
                .toArray(Expr[]::new)
        );
    }

    @Nonnull
    private Expr internalVisitBinaryOp(
        int type,
        @Nonnull List<DingoExprParser.ExprContext> exprList
    ) {
        Op op = OpFactory.getBinary(type);
        setParaList(op, exprList);
        return op;
    }

    @Nonnull
    private Expr internalVisitUnaryOp(
        int type,
        DingoExprParser.ExprContext expr
    ) {
        Op op = OpFactory.getUnary(type);
        op.setExprArray(new Expr[]{visit(expr)});
        return op;
    }

    @Nonnull
    @Override
    public Expr visitInt(@Nonnull DingoExprParser.IntContext ctx) {
        return Value.parseLong(ctx.INT().getText());
    }

    @Nonnull
    @Override
    public Expr visitReal(@Nonnull DingoExprParser.RealContext ctx) {
        return Value.parseDouble(ctx.REAL().getText());
    }

    @Nonnull
    @Override
    public Expr visitStr(@Nonnull DingoExprParser.StrContext ctx) {
        return Value.parseString(ctx.STR().getText());
    }

    @Nonnull
    @Override
    public Expr visitBool(@Nonnull DingoExprParser.BoolContext ctx) {
        return Value.parseBoolean(ctx.BOOL().getText());
    }

    @Nonnull
    @Override
    public Expr visitVar(@Nonnull DingoExprParser.VarContext ctx) {
        String name = ctx.ID().getText();
        if (name.equals("null") || name.equals("NULL")) {
            return Null.INSTANCE;
        }
        return new Var(ctx.ID().getText());
    }

    @Nonnull
    @Override
    public Expr visitPars(@Nonnull DingoExprParser.ParsContext ctx) {
        return visit(ctx.expr());
    }

    @Nonnull
    @Override
    public Expr visitPosNeg(@Nonnull DingoExprParser.PosNegContext ctx) {
        return internalVisitUnaryOp(ctx.op.getType(), ctx.expr());
    }

    @Nonnull
    @Override
    public Expr visitMulDiv(@Nonnull DingoExprParser.MulDivContext ctx) {
        return internalVisitBinaryOp(ctx.op.getType(), ctx.expr());
    }

    @Nonnull
    @Override
    public Expr visitAddSub(@Nonnull DingoExprParser.AddSubContext ctx) {
        return internalVisitBinaryOp(ctx.op.getType(), ctx.expr());
    }

    @Nonnull
    @Override
    public Expr visitRelation(@Nonnull DingoExprParser.RelationContext ctx) {
        return internalVisitBinaryOp(ctx.op.getType(), ctx.expr());
    }

    @Nonnull
    @Override
    public Expr visitNot(@Nonnull DingoExprParser.NotContext ctx) {
        return internalVisitUnaryOp(ctx.op.getType(), ctx.expr());
    }

    @Nonnull
    @Override
    public Expr visitAnd(@Nonnull DingoExprParser.AndContext ctx) {
        return internalVisitBinaryOp(ctx.op.getType(), ctx.expr());
    }

    @Nonnull
    @Override
    public Expr visitOr(@Nonnull DingoExprParser.OrContext ctx) {
        return internalVisitBinaryOp(ctx.op.getType(), ctx.expr());
    }

    @Nonnull
    @Override
    public Expr visitIndex(@Nonnull DingoExprParser.IndexContext ctx) {
        Op op = new IndexOp();
        setParaList(op, ctx.expr());
        return op;
    }

    @Nonnull
    @Override
    public Expr visitStrIndex(@Nonnull DingoExprParser.StrIndexContext ctx) {
        Op op = new IndexOp();
        op.setExprArray(new Expr[]{visit(ctx.expr()), Value.of(ctx.ID().getText())});
        return op;
    }

    @Nonnull
    @Override
    public Expr visitStringOp(@Nonnull DingoExprParser.StringOpContext ctx) {
        return internalVisitBinaryOp(ctx.op.getType(), ctx.expr());
    }

    @Nonnull
    @Override
    public Expr visitFun(@Nonnull DingoExprParser.FunContext ctx) {
        String funName = ctx.ID().getText();
        Op op = FunFactory.INS.getFun(funName);
        setParaList(op, ctx.expr());
        return op;
    }
}
