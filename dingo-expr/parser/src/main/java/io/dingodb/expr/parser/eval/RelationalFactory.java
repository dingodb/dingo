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
import io.dingodb.expr.runtime.eval.relational.eq.EqBool;
import io.dingodb.expr.runtime.eval.relational.eq.EqDouble;
import io.dingodb.expr.runtime.eval.relational.eq.EqFloat;
import io.dingodb.expr.runtime.eval.relational.eq.EqInt;
import io.dingodb.expr.runtime.eval.relational.eq.EqLong;
import io.dingodb.expr.runtime.eval.relational.eq.EqString;
import io.dingodb.expr.runtime.eval.relational.ge.GeBool;
import io.dingodb.expr.runtime.eval.relational.ge.GeDouble;
import io.dingodb.expr.runtime.eval.relational.ge.GeFloat;
import io.dingodb.expr.runtime.eval.relational.ge.GeInt;
import io.dingodb.expr.runtime.eval.relational.ge.GeLong;
import io.dingodb.expr.runtime.eval.relational.ge.GeString;
import io.dingodb.expr.runtime.eval.relational.gt.GtBool;
import io.dingodb.expr.runtime.eval.relational.gt.GtDouble;
import io.dingodb.expr.runtime.eval.relational.gt.GtFloat;
import io.dingodb.expr.runtime.eval.relational.gt.GtInt;
import io.dingodb.expr.runtime.eval.relational.gt.GtLong;
import io.dingodb.expr.runtime.eval.relational.gt.GtString;
import io.dingodb.expr.runtime.eval.relational.isFalse.IsFalseBool;
import io.dingodb.expr.runtime.eval.relational.isFalse.IsFalseDouble;
import io.dingodb.expr.runtime.eval.relational.isFalse.IsFalseFloat;
import io.dingodb.expr.runtime.eval.relational.isFalse.IsFalseInt;
import io.dingodb.expr.runtime.eval.relational.isFalse.IsFalseLong;
import io.dingodb.expr.runtime.eval.relational.isFalse.IsFalseString;
import io.dingodb.expr.runtime.eval.relational.isNull.IsNullBool;
import io.dingodb.expr.runtime.eval.relational.isNull.IsNullDouble;
import io.dingodb.expr.runtime.eval.relational.isNull.IsNullFloat;
import io.dingodb.expr.runtime.eval.relational.isNull.IsNullInt;
import io.dingodb.expr.runtime.eval.relational.isNull.IsNullLong;
import io.dingodb.expr.runtime.eval.relational.isNull.IsNullString;
import io.dingodb.expr.runtime.eval.relational.isTrue.IsTrueBool;
import io.dingodb.expr.runtime.eval.relational.isTrue.IsTrueDouble;
import io.dingodb.expr.runtime.eval.relational.isTrue.IsTrueFloat;
import io.dingodb.expr.runtime.eval.relational.isTrue.IsTrueInt;
import io.dingodb.expr.runtime.eval.relational.isTrue.IsTrueLong;
import io.dingodb.expr.runtime.eval.relational.isTrue.IsTrueString;
import io.dingodb.expr.runtime.eval.relational.le.LeBool;
import io.dingodb.expr.runtime.eval.relational.le.LeDouble;
import io.dingodb.expr.runtime.eval.relational.le.LeFloat;
import io.dingodb.expr.runtime.eval.relational.le.LeInt;
import io.dingodb.expr.runtime.eval.relational.le.LeLong;
import io.dingodb.expr.runtime.eval.relational.le.LeString;
import io.dingodb.expr.runtime.eval.relational.lt.LtBool;
import io.dingodb.expr.runtime.eval.relational.lt.LtDouble;
import io.dingodb.expr.runtime.eval.relational.lt.LtFloat;
import io.dingodb.expr.runtime.eval.relational.lt.LtInt;
import io.dingodb.expr.runtime.eval.relational.lt.LtLong;
import io.dingodb.expr.runtime.eval.relational.ne.NeBool;
import io.dingodb.expr.runtime.eval.relational.ne.NeDouble;
import io.dingodb.expr.runtime.eval.relational.ne.NeFloat;
import io.dingodb.expr.runtime.eval.relational.ne.NeInt;
import io.dingodb.expr.runtime.eval.relational.ne.NeLong;
import io.dingodb.expr.runtime.eval.relational.ne.NeString;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class RelationalFactory {
    private RelationalFactory() {
    }

    private static int bestRelationalType(int type0, int type1) {
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
        } else if (type0 == TypeCode.BOOL && type1 == TypeCode.BOOL) {
            return TypeCode.BOOL;
        } else if (type0 == TypeCode.STRING && type1 == TypeCode.STRING) {
            return TypeCode.STRING;
        }
        return -1;
    }

    public static @NonNull Eval eq(@NonNull Eval operand0, @NonNull Eval operand1) {
        int type0 = operand0.getType();
        int type1 = operand1.getType();
        int type = bestRelationalType(type0, type1);
        switch (type) {
            case TypeCode.INT:
                return new EqInt(CastingFactory.toInt(operand0), CastingFactory.toInt(operand1));
            case TypeCode.LONG:
                return new EqLong(CastingFactory.toLong(operand0), CastingFactory.toLong(operand1));
            case TypeCode.FLOAT:
                return new EqFloat(CastingFactory.toFloat(operand0), CastingFactory.toFloat(operand1));
            case TypeCode.DOUBLE:
                return new EqDouble(CastingFactory.toDouble(operand0), CastingFactory.toDouble(operand1));
            case TypeCode.BOOL:
                return new EqBool(CastingFactory.toBool(operand0), CastingFactory.toBool(operand1));
            case TypeCode.STRING:
                return new EqString(operand0, operand1);
            default:
                break;
        }
        throw new BinaryEvalException("EQ", type0, type1);
    }

    public static @NonNull Eval ge(@NonNull Eval operand0, @NonNull Eval operand1) {
        int type0 = operand0.getType();
        int type1 = operand1.getType();
        int type = bestRelationalType(type0, type1);
        switch (type) {
            case TypeCode.INT:
                return new GeInt(CastingFactory.toInt(operand0), CastingFactory.toInt(operand1));
            case TypeCode.LONG:
                return new GeLong(CastingFactory.toLong(operand0), CastingFactory.toLong(operand1));
            case TypeCode.FLOAT:
                return new GeFloat(CastingFactory.toFloat(operand0), CastingFactory.toFloat(operand1));
            case TypeCode.DOUBLE:
                return new GeDouble(CastingFactory.toDouble(operand0), CastingFactory.toDouble(operand1));
            case TypeCode.BOOL:
                return new GeBool(CastingFactory.toBool(operand0), CastingFactory.toBool(operand1));
            case TypeCode.STRING:
                return new GeString(operand0, operand1);
            default:
                break;
        }
        throw new BinaryEvalException("GE", type0, type1);
    }

    public static @NonNull Eval gt(@NonNull Eval operand0, @NonNull Eval operand1) {
        int type0 = operand0.getType();
        int type1 = operand1.getType();
        int type = bestRelationalType(type0, type1);
        switch (type) {
            case TypeCode.INT:
                return new GtInt(CastingFactory.toInt(operand0), CastingFactory.toInt(operand1));
            case TypeCode.LONG:
                return new GtLong(CastingFactory.toLong(operand0), CastingFactory.toLong(operand1));
            case TypeCode.FLOAT:
                return new GtFloat(CastingFactory.toFloat(operand0), CastingFactory.toFloat(operand1));
            case TypeCode.DOUBLE:
                return new GtDouble(CastingFactory.toDouble(operand0), CastingFactory.toDouble(operand1));
            case TypeCode.BOOL:
                return new GtBool(CastingFactory.toBool(operand0), CastingFactory.toBool(operand1));
            case TypeCode.STRING:
                return new GtString(operand0, operand1);
            default:
                break;
        }
        throw new BinaryEvalException("GT", type0, type1);
    }

    public static @NonNull Eval le(@NonNull Eval operand0, @NonNull Eval operand1) {
        int type0 = operand0.getType();
        int type1 = operand1.getType();
        int type = bestRelationalType(type0, type1);
        switch (type) {
            case TypeCode.INT:
                return new LeInt(CastingFactory.toInt(operand0), CastingFactory.toInt(operand1));
            case TypeCode.LONG:
                return new LeLong(CastingFactory.toLong(operand0), CastingFactory.toLong(operand1));
            case TypeCode.FLOAT:
                return new LeFloat(CastingFactory.toFloat(operand0), CastingFactory.toFloat(operand1));
            case TypeCode.DOUBLE:
                return new LeDouble(CastingFactory.toDouble(operand0), CastingFactory.toDouble(operand1));
            case TypeCode.BOOL:
                return new LeBool(CastingFactory.toBool(operand0), CastingFactory.toBool(operand1));
            case TypeCode.STRING:
                return new LeString(operand0, operand1);
            default:
                break;
        }
        throw new BinaryEvalException("LE", type0, type1);
    }

    public static @NonNull Eval lt(@NonNull Eval operand0, @NonNull Eval operand1) {
        int type0 = operand0.getType();
        int type1 = operand1.getType();
        int type = bestRelationalType(type0, type1);
        switch (type) {
            case TypeCode.INT:
                return new LtInt(CastingFactory.toInt(operand0), CastingFactory.toInt(operand1));
            case TypeCode.LONG:
                return new LtLong(CastingFactory.toLong(operand0), CastingFactory.toLong(operand1));
            case TypeCode.FLOAT:
                return new LtFloat(CastingFactory.toFloat(operand0), CastingFactory.toFloat(operand1));
            case TypeCode.DOUBLE:
                return new LtDouble(CastingFactory.toDouble(operand0), CastingFactory.toDouble(operand1));
            case TypeCode.BOOL:
                return new LtBool(CastingFactory.toBool(operand0), CastingFactory.toBool(operand1));
            case TypeCode.STRING:
                return new EqString(operand0, operand1);
            default:
                break;
        }
        throw new BinaryEvalException("LT", type0, type1);
    }

    public static @NonNull Eval ne(@NonNull Eval operand0, @NonNull Eval operand1) {
        int type0 = operand0.getType();
        int type1 = operand1.getType();
        int type = bestRelationalType(type0, type1);
        switch (type) {
            case TypeCode.INT:
                return new NeInt(CastingFactory.toInt(operand0), CastingFactory.toInt(operand1));
            case TypeCode.LONG:
                return new NeLong(CastingFactory.toLong(operand0), CastingFactory.toLong(operand1));
            case TypeCode.FLOAT:
                return new NeFloat(CastingFactory.toFloat(operand0), CastingFactory.toFloat(operand1));
            case TypeCode.DOUBLE:
                return new NeDouble(CastingFactory.toDouble(operand0), CastingFactory.toDouble(operand1));
            case TypeCode.BOOL:
                return new NeBool(CastingFactory.toBool(operand0), CastingFactory.toBool(operand1));
            case TypeCode.STRING:
                return new NeString(operand0, operand1);
            default:
                break;
        }
        throw new BinaryEvalException("EQ", type0, type1);
    }

    public static @NonNull Eval isNull(@NonNull Eval operand) {
        int type = operand.getType();
        switch (type) {
            case TypeCode.INT:
                return new IsNullInt(operand);
            case TypeCode.LONG:
                return new IsNullLong(operand);
            case TypeCode.FLOAT:
                return new IsNullFloat(operand);
            case TypeCode.DOUBLE:
                return new IsNullDouble(operand);
            case TypeCode.BOOL:
                return new IsNullBool(operand);
            case TypeCode.STRING:
                return new IsNullString(operand);
            default:
                break;
        }
        throw new UnaryEvalException("IS_NULL", type);
    }

    public static @NonNull Eval isTrue(@NonNull Eval operand) {
        int type = operand.getType();
        switch (type) {
            case TypeCode.INT:
                return new IsTrueInt(operand);
            case TypeCode.LONG:
                return new IsTrueLong(operand);
            case TypeCode.FLOAT:
                return new IsTrueFloat(operand);
            case TypeCode.DOUBLE:
                return new IsTrueDouble(operand);
            case TypeCode.BOOL:
                return new IsTrueBool(operand);
            case TypeCode.STRING:
                return new IsTrueString(operand);
            default:
                break;
        }
        throw new UnaryEvalException("IS_TRUE", type);
    }

    public static @NonNull Eval isFalse(@NonNull Eval operand) {
        int type = operand.getType();
        switch (type) {
            case TypeCode.INT:
                return new IsFalseInt(operand);
            case TypeCode.LONG:
                return new IsFalseLong(operand);
            case TypeCode.FLOAT:
                return new IsFalseFloat(operand);
            case TypeCode.DOUBLE:
                return new IsFalseDouble(operand);
            case TypeCode.BOOL:
                return new IsFalseBool(operand);
            case TypeCode.STRING:
                return new IsFalseString(operand);
            default:
                break;
        }
        throw new UnaryEvalException("IS_FALSE", type);
    }
}
