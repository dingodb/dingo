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

import io.dingodb.expr.core.TypeCode;
import io.dingodb.expr.parser.eval.ArithmeticFactory;
import io.dingodb.expr.parser.eval.CastingFactory;
import io.dingodb.expr.parser.eval.IndexFactory;
import io.dingodb.expr.parser.eval.LogicalFactory;
import io.dingodb.expr.parser.eval.RelationalFactory;
import io.dingodb.expr.parser.eval.VarFactory;
import io.dingodb.expr.parser.op.AndOp;
import io.dingodb.expr.parser.op.IsFalseOp;
import io.dingodb.expr.parser.op.IsNotFalseOp;
import io.dingodb.expr.parser.op.IsNotNullOp;
import io.dingodb.expr.parser.op.IsNotTrueOp;
import io.dingodb.expr.parser.op.IsNullOp;
import io.dingodb.expr.parser.op.IsTrueOp;
import io.dingodb.expr.parser.op.Op;
import io.dingodb.expr.parser.op.OrOp;
import io.dingodb.expr.parser.value.Null;
import io.dingodb.expr.parser.value.Value;
import io.dingodb.expr.parser.var.Var;
import io.dingodb.expr.runtime.CompileContext;
import io.dingodb.expr.runtime.eval.Eval;
import io.dingodb.expr.runtime.eval.value.BoolValue;
import io.dingodb.expr.runtime.eval.value.DoubleValue;
import io.dingodb.expr.runtime.eval.value.FloatValue;
import io.dingodb.expr.runtime.eval.value.IntValue;
import io.dingodb.expr.runtime.eval.value.LongValue;
import io.dingodb.expr.runtime.eval.value.StringValue;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@RequiredArgsConstructor(access = AccessLevel.PUBLIC)
public class ExprCompiler implements ExprVisitor<Eval> {
    public final static ExprCompiler SIMPLE = new ExprCompiler(null);

    private final CompileContext context;

    public static ExprCompiler of(CompileContext context) {
        if (context != null) {
            return new ExprCompiler(context);
        }
        return SIMPLE;
    }

    @Override
    public Eval visit(@NonNull Null op) {
        return null;
    }

    @Override
    public <V> Eval visit(@NonNull Value<V> op) {
        switch (TypeCode.getTypeCode(op.getValue())) {
            case TypeCode.INT:
                return new IntValue((Integer) op.getValue());
            case TypeCode.LONG:
                return new LongValue((Long) op.getValue());
            case TypeCode.FLOAT:
                return new FloatValue((Float) op.getValue());
            case TypeCode.DOUBLE:
                return new DoubleValue((Double) op.getValue());
            case TypeCode.BOOL:
                return BoolValue.of((Boolean) op.getValue());
            case TypeCode.STRING:
                return new StringValue((String) op.getValue());
        }
        return null;
    }

    @Override
    public Eval visit(@NonNull Op op) {
        List<Eval> evalList = Arrays.stream(op.getExprArray())
            .map(e -> e.accept(this))
            .collect(Collectors.toList());
        if (evalList.stream().anyMatch(Objects::isNull)) {
            return null;
        }
        switch (op.getType()) {
            case NOT:
                return LogicalFactory.not(evalList.get(0));
            case AND:
                return LogicalFactory.and(evalList.get(0), evalList.get(1));
            case OR:
                return LogicalFactory.or(evalList.get(0), evalList.get(1));
            case POS:
                return ArithmeticFactory.pos(evalList.get(0));
            case NEG:
                return ArithmeticFactory.neg(evalList.get(0));
            case ADD:
                return ArithmeticFactory.add(evalList.get(0), evalList.get(1));
            case SUB:
                return ArithmeticFactory.sub(evalList.get(0), evalList.get(1));
            case MUL:
                return ArithmeticFactory.mul(evalList.get(0), evalList.get(1));
            case DIV:
                return ArithmeticFactory.div(evalList.get(0), evalList.get(1));
            case EQ:
                return RelationalFactory.eq(evalList.get(0), evalList.get(1));
            case GE:
                return RelationalFactory.ge(evalList.get(0), evalList.get(1));
            case GT:
                return RelationalFactory.gt(evalList.get(0), evalList.get(1));
            case LE:
                return RelationalFactory.le(evalList.get(0), evalList.get(1));
            case LT:
                return RelationalFactory.lt(evalList.get(0), evalList.get(1));
            case NE:
                return RelationalFactory.ne(evalList.get(0), evalList.get(1));
            case FUN:
                switch (op.getName()) {
                    case AndOp.FUN_NAME:
                        return LogicalFactory.varAnd(evalList);
                    case OrOp.FUN_NAME:
                        return LogicalFactory.varOr(evalList);
                    case IsNullOp.FUN_NAME:
                        return RelationalFactory.isNull(evalList.get((0)));
                    case IsNotNullOp.FUN_NAME:
                        return LogicalFactory.not(RelationalFactory.isNull(evalList.get((0))));
                    case IsTrueOp.FUN_NAME:
                        return RelationalFactory.isTrue(evalList.get((0)));
                    case IsNotTrueOp.FUN_NAME:
                        return LogicalFactory.not(RelationalFactory.isTrue(evalList.get((0))));
                    case IsFalseOp.FUN_NAME:
                        return RelationalFactory.isFalse(evalList.get((0)));
                    case IsNotFalseOp.FUN_NAME:
                        return LogicalFactory.not(RelationalFactory.isFalse(evalList.get((0))));
                    case TypeCode.INT_NAME:
                        return CastingFactory.toInt(evalList.get(0));
                    case TypeCode.LONG_NAME:
                        return CastingFactory.toLong(evalList.get(0));
                    case TypeCode.BOOL_NAME:
                        return CastingFactory.toBool(evalList.get(0));
                    case TypeCode.FLOAT_NAME:
                        return CastingFactory.toFloat(evalList.get(0));
                    case TypeCode.DOUBLE_NAME:
                        return CastingFactory.toDouble(evalList.get(0));
                    default:
                        break;
                }
                break;
            case INDEX:
                return IndexFactory.of(evalList.get(0), evalList.get(1));
            default:
                break;
        }
        return null;
    }

    @Override
    public Eval visit(@NonNull Var op) {
        return VarFactory.of(op.getName(), context);
    }
}
