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
import io.dingodb.expr.parser.exception.TypeCastingException;
import io.dingodb.expr.runtime.eval.Eval;
import io.dingodb.expr.runtime.eval.cast.DoubleToBool;
import io.dingodb.expr.runtime.eval.cast.DoubleToFloat;
import io.dingodb.expr.runtime.eval.cast.DoubleToInt;
import io.dingodb.expr.runtime.eval.cast.DoubleToLong;
import io.dingodb.expr.runtime.eval.cast.FloatToBool;
import io.dingodb.expr.runtime.eval.cast.FloatToDouble;
import io.dingodb.expr.runtime.eval.cast.FloatToInt;
import io.dingodb.expr.runtime.eval.cast.FloatToLong;
import io.dingodb.expr.runtime.eval.cast.IntToBool;
import io.dingodb.expr.runtime.eval.cast.IntToDouble;
import io.dingodb.expr.runtime.eval.cast.IntToFloat;
import io.dingodb.expr.runtime.eval.cast.IntToLong;
import io.dingodb.expr.runtime.eval.cast.LongToBool;
import io.dingodb.expr.runtime.eval.cast.LongToDouble;
import io.dingodb.expr.runtime.eval.cast.LongToFloat;
import io.dingodb.expr.runtime.eval.cast.LongToInt;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class CastingFactory {
    private CastingFactory() {
    }

    public static @NonNull Eval toInt(@NonNull Eval operand) {
        int type = operand.getType();
        switch (type) {
            case TypeCode.INT:
                return operand;
            case TypeCode.LONG:
                return new LongToInt(operand);
            case TypeCode.FLOAT:
                return new FloatToInt(operand);
            case TypeCode.DOUBLE:
                return new DoubleToInt(operand);
            default:
                break;
        }
        throw new TypeCastingException(type, TypeCode.INT);
    }

    public static @NonNull Eval toLong(@NonNull Eval operand) {
        int type = operand.getType();
        switch (type) {
            case TypeCode.INT:
                return new IntToLong(operand);
            case TypeCode.LONG:
                return operand;
            case TypeCode.FLOAT:
                return new FloatToLong(operand);
            case TypeCode.DOUBLE:
                return new DoubleToLong(operand);
            default:
                break;
        }
        throw new TypeCastingException(type, TypeCode.LONG);
    }

    public static @NonNull Eval toFloat(@NonNull Eval operand) {
        int type = operand.getType();
        switch (type) {
            case TypeCode.INT:
                return new IntToFloat(operand);
            case TypeCode.LONG:
                return new LongToFloat(operand);
            case TypeCode.FLOAT:
                return operand;
            case TypeCode.DOUBLE:
                return new DoubleToFloat(operand);
            default:
                break;
        }
        throw new TypeCastingException(type, TypeCode.FLOAT);
    }

    public static @NonNull Eval toDouble(@NonNull Eval operand) {
        int type = operand.getType();
        switch (type) {
            case TypeCode.INT:
                return new IntToDouble(operand);
            case TypeCode.LONG:
                return new LongToDouble(operand);
            case TypeCode.FLOAT:
                return new FloatToDouble(operand);
            case TypeCode.DOUBLE:
                return operand;
            default:
                break;
        }
        throw new TypeCastingException(type, TypeCode.DOUBLE);
    }

    public static @NonNull Eval toBool(@NonNull Eval operand) {
        int type = operand.getType();
        switch (type) {
            case TypeCode.INT:
                return new IntToBool(operand);
            case TypeCode.LONG:
                return new LongToBool(operand);
            case TypeCode.FLOAT:
                return new FloatToBool(operand);
            case TypeCode.DOUBLE:
                return new DoubleToBool(operand);
            case TypeCode.BOOL:
                return operand;
            default:
                break;
        }
        throw new TypeCastingException(type, TypeCode.INT);
    }
}
