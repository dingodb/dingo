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

package io.dingodb.expr.parser.eval;

import io.dingodb.expr.core.TypeCode;
import io.dingodb.expr.parser.exception.BinaryEvalException;
import io.dingodb.expr.parser.exception.UnaryEvalException;
import io.dingodb.expr.runtime.eval.Eval;
import io.dingodb.expr.runtime.eval.arithmetic.AddDouble;
import io.dingodb.expr.runtime.eval.arithmetic.AddFloat;
import io.dingodb.expr.runtime.eval.arithmetic.AddInt;
import io.dingodb.expr.runtime.eval.arithmetic.AddLong;
import io.dingodb.expr.runtime.eval.arithmetic.DivDouble;
import io.dingodb.expr.runtime.eval.arithmetic.DivFloat;
import io.dingodb.expr.runtime.eval.arithmetic.DivInt;
import io.dingodb.expr.runtime.eval.arithmetic.DivLong;
import io.dingodb.expr.runtime.eval.arithmetic.MulDouble;
import io.dingodb.expr.runtime.eval.arithmetic.MulFloat;
import io.dingodb.expr.runtime.eval.arithmetic.MulInt;
import io.dingodb.expr.runtime.eval.arithmetic.MulLong;
import io.dingodb.expr.runtime.eval.arithmetic.NegDouble;
import io.dingodb.expr.runtime.eval.arithmetic.NegFloat;
import io.dingodb.expr.runtime.eval.arithmetic.NegInt;
import io.dingodb.expr.runtime.eval.arithmetic.NegLong;
import io.dingodb.expr.runtime.eval.arithmetic.PosDouble;
import io.dingodb.expr.runtime.eval.arithmetic.PosFloat;
import io.dingodb.expr.runtime.eval.arithmetic.PosInt;
import io.dingodb.expr.runtime.eval.arithmetic.PosLong;
import io.dingodb.expr.runtime.eval.arithmetic.SubDouble;
import io.dingodb.expr.runtime.eval.arithmetic.SubFloat;
import io.dingodb.expr.runtime.eval.arithmetic.SubInt;
import io.dingodb.expr.runtime.eval.arithmetic.SubLong;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class ArithmeticFactory {
    private ArithmeticFactory() {
    }

    private static int bestNumericalType(int type0, int type1) {
        if (type0 == TypeCode.DECIMAL || type1 == TypeCode.DECIMAL) {
            return TypeCode.DECIMAL;
        } else if (type0 == TypeCode.DOUBLE || type1 == TypeCode.DOUBLE) {
            return TypeCode.DOUBLE;
        } else if (type0 == TypeCode.FLOAT || type1 == TypeCode.FLOAT) {
            return TypeCode.FLOAT;
        } else if (type0 == TypeCode.LONG || type1 == TypeCode.LONG) {
            return TypeCode.LONG;
        } else if (type0 == TypeCode.INT && type1 == TypeCode.INT) {
            return TypeCode.INT;
        }
        return -1;
    }

    public static @NonNull Eval pos(@NonNull Eval operand) {
        int type = operand.getType();
        switch (type) {
            case TypeCode.INT:
                return new PosInt(operand);
            case TypeCode.LONG:
                return new PosLong(operand);
            case TypeCode.FLOAT:
                return new PosFloat(operand);
            case TypeCode.DOUBLE:
                return new PosDouble(operand);
            default:
                break;
        }
        throw new UnaryEvalException("POS", type);
    }

    public static @NonNull Eval neg(@NonNull Eval operand) {
        int type = operand.getType();
        switch (type) {
            case TypeCode.INT:
                return new NegInt(operand);
            case TypeCode.LONG:
                return new NegLong(operand);
            case TypeCode.FLOAT:
                return new NegFloat(operand);
            case TypeCode.DOUBLE:
                return new NegDouble(operand);
            default:
                break;
        }
        throw new UnaryEvalException("NEG", type);
    }

    public static @NonNull Eval add(@NonNull Eval operand0, @NonNull Eval operand1) {
        int type0 = operand0.getType();
        int type1 = operand1.getType();
        int type = bestNumericalType(type0, type1);
        switch (type) {
            case TypeCode.INT:
                return new AddInt(CastingFactory.toInt(operand0), CastingFactory.toInt(operand1));
            case TypeCode.LONG:
                return new AddLong(CastingFactory.toLong(operand0), CastingFactory.toLong(operand1));
            case TypeCode.FLOAT:
                return new AddFloat(CastingFactory.toFloat(operand0), CastingFactory.toFloat(operand1));
            case TypeCode.DOUBLE:
                return new AddDouble(CastingFactory.toDouble(operand0), CastingFactory.toDouble(operand1));
            default:
                break;
        }
        throw new BinaryEvalException("ADD", type0, type1);
    }

    public static @NonNull Eval sub(@NonNull Eval operand0, @NonNull Eval operand1) {
        int type0 = operand0.getType();
        int type1 = operand1.getType();
        int type = bestNumericalType(type0, type1);
        switch (type) {
            case TypeCode.INT:
                return new SubInt(CastingFactory.toInt(operand0), CastingFactory.toInt(operand1));
            case TypeCode.LONG:
                return new SubLong(CastingFactory.toLong(operand0), CastingFactory.toLong(operand1));
            case TypeCode.FLOAT:
                return new SubFloat(CastingFactory.toFloat(operand0), CastingFactory.toFloat(operand1));
            case TypeCode.DOUBLE:
                return new SubDouble(CastingFactory.toDouble(operand0), CastingFactory.toDouble(operand1));
            default:
                break;
        }
        throw new BinaryEvalException("SUB", type0, type1);
    }

    public static @NonNull Eval mul(@NonNull Eval operand0, @NonNull Eval operand1) {
        int type0 = operand0.getType();
        int type1 = operand1.getType();
        int type = bestNumericalType(type0, type1);
        switch (type) {
            case TypeCode.INT:
                return new MulInt(CastingFactory.toInt(operand0), CastingFactory.toInt(operand1));
            case TypeCode.LONG:
                return new MulLong(CastingFactory.toLong(operand0), CastingFactory.toLong(operand1));
            case TypeCode.FLOAT:
                return new MulFloat(CastingFactory.toFloat(operand0), CastingFactory.toFloat(operand1));
            case TypeCode.DOUBLE:
                return new MulDouble(CastingFactory.toDouble(operand0), CastingFactory.toDouble(operand1));
            default:
                break;
        }
        throw new BinaryEvalException("MUL", type0, type1);
    }

    public static @NonNull Eval div(@NonNull Eval operand0, @NonNull Eval operand1) {
        int type0 = operand0.getType();
        int type1 = operand1.getType();
        int type = bestNumericalType(type0, type1);
        switch (type) {
            case TypeCode.INT:
                return new DivInt(CastingFactory.toInt(operand0), CastingFactory.toInt(operand1));
            case TypeCode.LONG:
                return new DivLong(CastingFactory.toLong(operand0), CastingFactory.toLong(operand1));
            case TypeCode.FLOAT:
                return new DivFloat(CastingFactory.toFloat(operand0), CastingFactory.toFloat(operand1));
            case TypeCode.DOUBLE:
                return new DivDouble(CastingFactory.toDouble(operand0), CastingFactory.toDouble(operand1));
            default:
                break;
        }
        throw new BinaryEvalException("DIV", type0, type1);
    }
}
