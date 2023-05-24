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

package io.dingodb.expr.parser;

import io.dingodb.expr.parser.op.AndOp;
import io.dingodb.expr.parser.op.Op;
import io.dingodb.expr.parser.op.OpType;
import io.dingodb.expr.parser.op.OpWithEvaluator;
import io.dingodb.expr.parser.op.OrOp;
import io.dingodb.expr.runtime.evaluator.arithmetic.AddEvaluatorsFactory;
import io.dingodb.expr.runtime.evaluator.arithmetic.DivEvaluatorsFactory;
import io.dingodb.expr.runtime.evaluator.arithmetic.MulEvaluatorsFactory;
import io.dingodb.expr.runtime.evaluator.arithmetic.NegEvaluatorsFactory;
import io.dingodb.expr.runtime.evaluator.arithmetic.PosEvaluatorsFactory;
import io.dingodb.expr.runtime.evaluator.arithmetic.SubEvaluatorsFactory;
import io.dingodb.expr.runtime.evaluator.relational.EqEvaluatorsFactory;
import io.dingodb.expr.runtime.evaluator.relational.GeEvaluatorsFactory;
import io.dingodb.expr.runtime.evaluator.relational.GtEvaluatorsFactory;
import io.dingodb.expr.runtime.evaluator.relational.LeEvaluatorsFactory;
import io.dingodb.expr.runtime.evaluator.relational.LtEvaluatorsFactory;
import io.dingodb.expr.runtime.evaluator.relational.NeEvaluatorsFactory;
import io.dingodb.expr.runtime.op.logical.RtNotOp;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class OpFactory {
    private OpFactory() {
    }

    /**
     * Get a unary Op by its type.
     *
     * @param type the type
     * @return the Op
     */
    public static @NonNull Op getUnary(int type) {
        switch (type) {
            case DingoExprParser.ADD:
                return new OpWithEvaluator(OpType.POS, PosEvaluatorsFactory.INSTANCE);
            case DingoExprParser.SUB:
                return new OpWithEvaluator(OpType.NEG, NegEvaluatorsFactory.INSTANCE);
            case DingoExprParser.NOT:
                return new RtOpWrapper(OpType.NOT, RtNotOp::new);
            default:
                throw new ParseCancellationException("Invalid operator type: " + type);
        }
    }

    /**
     * Get a binary Op by its type.
     *
     * @param type the type
     * @return the Op
     */
    public static @NonNull Op getBinary(int type) {
        switch (type) {
            case DingoExprParser.ADD:
                return new OpWithEvaluator(OpType.ADD, AddEvaluatorsFactory.INSTANCE);
            case DingoExprParser.SUB:
                return new OpWithEvaluator(OpType.SUB, SubEvaluatorsFactory.INSTANCE);
            case DingoExprParser.MUL:
                return new OpWithEvaluator(OpType.MUL, MulEvaluatorsFactory.INSTANCE);
            case DingoExprParser.DIV:
                return new OpWithEvaluator(OpType.DIV, DivEvaluatorsFactory.INSTANCE);
            case DingoExprParser.LT:
                return new OpWithEvaluator(OpType.LT, LtEvaluatorsFactory.INSTANCE);
            case DingoExprParser.LE:
                return new OpWithEvaluator(OpType.LE, LeEvaluatorsFactory.INSTANCE);
            case DingoExprParser.EQ:
                return new OpWithEvaluator(OpType.EQ, EqEvaluatorsFactory.INSTANCE);
            case DingoExprParser.GT:
                return new OpWithEvaluator(OpType.GT, GtEvaluatorsFactory.INSTANCE);
            case DingoExprParser.GE:
                return new OpWithEvaluator(OpType.GE, GeEvaluatorsFactory.INSTANCE);
            case DingoExprParser.NE:
                return new OpWithEvaluator(OpType.NE, NeEvaluatorsFactory.INSTANCE);
            case DingoExprParser.AND:
                return AndOp.op();
            case DingoExprParser.OR:
                return OrOp.op();
            default:
                throw new ParseCancellationException("Invalid operator type: " + type);
        }
    }
}
