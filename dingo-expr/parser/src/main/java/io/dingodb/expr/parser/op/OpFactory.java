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

import io.dingodb.expr.parser.DingoExprParser;
import io.dingodb.expr.runtime.evaluator.arithmetic.AddEvaluatorFactory;
import io.dingodb.expr.runtime.evaluator.arithmetic.DivEvaluatorFactory;
import io.dingodb.expr.runtime.evaluator.arithmetic.MulEvaluatorFactory;
import io.dingodb.expr.runtime.evaluator.arithmetic.NegEvaluatorFactory;
import io.dingodb.expr.runtime.evaluator.arithmetic.PosEvaluatorFactory;
import io.dingodb.expr.runtime.evaluator.arithmetic.SubEvaluatorFactory;
import io.dingodb.expr.runtime.evaluator.relational.EqEvaluatorFactory;
import io.dingodb.expr.runtime.evaluator.relational.GeEvaluatorFactory;
import io.dingodb.expr.runtime.evaluator.relational.GtEvaluatorFactory;
import io.dingodb.expr.runtime.evaluator.relational.LeEvaluatorFactory;
import io.dingodb.expr.runtime.evaluator.relational.LtEvaluatorFactory;
import io.dingodb.expr.runtime.evaluator.relational.NeEvaluatorFactory;
import io.dingodb.expr.runtime.op.logical.RtNotOp;
import io.dingodb.expr.runtime.op.string.RtContainsOp;
import io.dingodb.expr.runtime.op.string.RtEndsWithOp;
import io.dingodb.expr.runtime.op.string.RtMatchesOp;
import io.dingodb.expr.runtime.op.string.RtStartsWithOp;
import org.antlr.v4.runtime.misc.ParseCancellationException;

import javax.annotation.Nonnull;

public final class OpFactory {
    private OpFactory() {
    }

    /**
     * Get an unary Op by its type.
     *
     * @param type the type
     * @return the Op
     */
    @Nonnull
    public static Op getUnary(int type) {
        switch (type) {
            case DingoExprParser.ADD:
                return new OpWithEvaluator(OpType.POS, PosEvaluatorFactory.INSTANCE);
            case DingoExprParser.SUB:
                return new OpWithEvaluator(OpType.NEG, NegEvaluatorFactory.INSTANCE);
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
    @Nonnull
    public static Op getBinary(int type) {
        switch (type) {
            case DingoExprParser.ADD:
                return new OpWithEvaluator(OpType.ADD, AddEvaluatorFactory.INSTANCE);
            case DingoExprParser.SUB:
                return new OpWithEvaluator(OpType.SUB, SubEvaluatorFactory.INSTANCE);
            case DingoExprParser.MUL:
                return new OpWithEvaluator(OpType.MUL, MulEvaluatorFactory.INSTANCE);
            case DingoExprParser.DIV:
                return new OpWithEvaluator(OpType.DIV, DivEvaluatorFactory.INSTANCE);
            case DingoExprParser.LT:
                return new OpWithEvaluator(OpType.LT, LtEvaluatorFactory.INSTANCE);
            case DingoExprParser.LE:
                return new OpWithEvaluator(OpType.LE, LeEvaluatorFactory.INSTANCE);
            case DingoExprParser.EQ:
                return new OpWithEvaluator(OpType.EQ, EqEvaluatorFactory.INSTANCE);
            case DingoExprParser.GT:
                return new OpWithEvaluator(OpType.GT, GtEvaluatorFactory.INSTANCE);
            case DingoExprParser.GE:
                return new OpWithEvaluator(OpType.GE, GeEvaluatorFactory.INSTANCE);
            case DingoExprParser.NE:
                return new OpWithEvaluator(OpType.NE, NeEvaluatorFactory.INSTANCE);
            case DingoExprParser.AND:
                return AndOp.op();
            case DingoExprParser.OR:
                return OrOp.op();
            case DingoExprParser.STARTS_WITH:
                return new RtOpWrapper(OpType.STARTS_WITH, RtStartsWithOp::new);
            case DingoExprParser.ENDS_WITH:
                return new RtOpWrapper(OpType.ENDS_WITH, RtEndsWithOp::new);
            case DingoExprParser.CONTAINS:
                return new RtOpWrapper(OpType.CONTAINS, RtContainsOp::new);
            case DingoExprParser.MATCHES:
                return new RtOpWrapper(OpType.MATCHES, RtMatchesOp::new);
            default:
                throw new ParseCancellationException("Invalid operator type: " + type);
        }
    }
}
