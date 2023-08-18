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

package io.dingodb.exec.expr;

import io.dingodb.exec.utils.CodecUtils;
import io.dingodb.expr.core.TypeCode;
import io.dingodb.expr.parser.exception.TemporarilyUnsupported;
import io.dingodb.expr.runtime.eval.BinaryEval;
import io.dingodb.expr.runtime.eval.Eval;
import io.dingodb.expr.runtime.eval.EvalVisitor;
import io.dingodb.expr.runtime.eval.UnaryEval;
import io.dingodb.expr.runtime.eval.arithmetic.add.AddDouble;
import io.dingodb.expr.runtime.eval.arithmetic.add.AddFloat;
import io.dingodb.expr.runtime.eval.arithmetic.add.AddInt;
import io.dingodb.expr.runtime.eval.arithmetic.add.AddLong;
import io.dingodb.expr.runtime.eval.arithmetic.div.DivDouble;
import io.dingodb.expr.runtime.eval.arithmetic.div.DivFloat;
import io.dingodb.expr.runtime.eval.arithmetic.div.DivInt;
import io.dingodb.expr.runtime.eval.arithmetic.div.DivLong;
import io.dingodb.expr.runtime.eval.arithmetic.mul.MulDouble;
import io.dingodb.expr.runtime.eval.arithmetic.mul.MulFloat;
import io.dingodb.expr.runtime.eval.arithmetic.mul.MulInt;
import io.dingodb.expr.runtime.eval.arithmetic.mul.MulLong;
import io.dingodb.expr.runtime.eval.arithmetic.neg.NegDouble;
import io.dingodb.expr.runtime.eval.arithmetic.neg.NegFloat;
import io.dingodb.expr.runtime.eval.arithmetic.neg.NegInt;
import io.dingodb.expr.runtime.eval.arithmetic.neg.NegLong;
import io.dingodb.expr.runtime.eval.arithmetic.pos.PosDouble;
import io.dingodb.expr.runtime.eval.arithmetic.pos.PosFloat;
import io.dingodb.expr.runtime.eval.arithmetic.pos.PosInt;
import io.dingodb.expr.runtime.eval.arithmetic.pos.PosLong;
import io.dingodb.expr.runtime.eval.arithmetic.sub.SubDouble;
import io.dingodb.expr.runtime.eval.arithmetic.sub.SubFloat;
import io.dingodb.expr.runtime.eval.arithmetic.sub.SubInt;
import io.dingodb.expr.runtime.eval.arithmetic.sub.SubLong;
import io.dingodb.expr.runtime.eval.cast.toBool.DoubleToBool;
import io.dingodb.expr.runtime.eval.cast.toBool.FloatToBool;
import io.dingodb.expr.runtime.eval.cast.toBool.IntToBool;
import io.dingodb.expr.runtime.eval.cast.toBool.LongToBool;
import io.dingodb.expr.runtime.eval.cast.toDouble.FloatToDouble;
import io.dingodb.expr.runtime.eval.cast.toDouble.IntToDouble;
import io.dingodb.expr.runtime.eval.cast.toDouble.LongToDouble;
import io.dingodb.expr.runtime.eval.cast.toFloat.DoubleToFloat;
import io.dingodb.expr.runtime.eval.cast.toFloat.IntToFloat;
import io.dingodb.expr.runtime.eval.cast.toFloat.LongToFloat;
import io.dingodb.expr.runtime.eval.cast.toInt.DoubleToInt;
import io.dingodb.expr.runtime.eval.cast.toInt.FloatToInt;
import io.dingodb.expr.runtime.eval.cast.toInt.LongToInt;
import io.dingodb.expr.runtime.eval.cast.toLong.DoubleToLong;
import io.dingodb.expr.runtime.eval.cast.toLong.FloatToLong;
import io.dingodb.expr.runtime.eval.cast.toLong.IntToLong;
import io.dingodb.expr.runtime.eval.logical.AndEval;
import io.dingodb.expr.runtime.eval.logical.NotEval;
import io.dingodb.expr.runtime.eval.logical.OrEval;
import io.dingodb.expr.runtime.eval.logical.VarArgAndEval;
import io.dingodb.expr.runtime.eval.logical.VarArgOrEval;
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
import io.dingodb.expr.runtime.eval.relational.lt.LtString;
import io.dingodb.expr.runtime.eval.relational.ne.NeBool;
import io.dingodb.expr.runtime.eval.relational.ne.NeDouble;
import io.dingodb.expr.runtime.eval.relational.ne.NeFloat;
import io.dingodb.expr.runtime.eval.relational.ne.NeInt;
import io.dingodb.expr.runtime.eval.relational.ne.NeLong;
import io.dingodb.expr.runtime.eval.relational.ne.NeString;
import io.dingodb.expr.runtime.eval.value.BoolValue;
import io.dingodb.expr.runtime.eval.value.DoubleValue;
import io.dingodb.expr.runtime.eval.value.FloatValue;
import io.dingodb.expr.runtime.eval.value.IntValue;
import io.dingodb.expr.runtime.eval.value.LongValue;
import io.dingodb.expr.runtime.eval.value.StringValue;
import io.dingodb.expr.runtime.eval.var.IndexedVar;
import io.dingodb.expr.runtime.eval.var.NamedVar;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

@RequiredArgsConstructor(access = AccessLevel.PUBLIC)
public class EvalSerializer implements EvalVisitor<Boolean> {
    private static final byte TYPE_INT32 = (byte) 0x01;
    private static final byte TYPE_INT64 = (byte) 0x02;
    private static final byte TYPE_BOOL = (byte) 0x03;
    private static final byte TYPE_FLOAT = (byte) 0x04;
    private static final byte TYPE_DOUBLE = (byte) 0x05;
    private static final byte TYPE_DECIMAL = (byte) 0x06;
    private static final byte TYPE_STRING = (byte) 0x07;

    private static final byte NULL = (byte) 0x00;
    private static final byte NULL_INT32 = NULL | TYPE_INT32;
    private static final byte NULL_INT64 = NULL | TYPE_INT64;
    private static final byte NULL_BOOL = NULL | TYPE_BOOL;
    private static final byte NULL_FLOAT = NULL | TYPE_FLOAT;
    private static final byte NULL_DOUBLE = NULL | TYPE_DOUBLE;
    private static final byte NULL_DECIMAL = NULL | TYPE_DECIMAL;
    private static final byte NULL_STRING = NULL | TYPE_STRING;

    private static final byte CONST = (byte) 0x10;
    private static final byte CONST_INT32 = CONST | TYPE_INT32;
    private static final byte CONST_INT64 = CONST | TYPE_INT64;
    private static final byte CONST_BOOL = CONST | TYPE_BOOL;
    private static final byte CONST_FLOAT = CONST | TYPE_FLOAT;
    private static final byte CONST_DOUBLE = CONST | TYPE_DOUBLE;
    private static final byte CONST_DECIMAL = CONST | TYPE_DECIMAL;
    private static final byte CONST_STRING = CONST | TYPE_STRING;

    private static final byte CONST_N = (byte) 0x20;
    private static final byte CONST_N_INT32 = CONST_N | TYPE_INT32;
    private static final byte CONST_N_INT64 = CONST_N | TYPE_INT64;
    private static final byte CONST_N_BOOL = CONST_N | TYPE_BOOL;

    private static final byte VAR_I = (byte) 0x30;
    private static final byte VAR_I_INT32 = VAR_I | TYPE_INT32;
    private static final byte VAR_I_INT64 = VAR_I | TYPE_INT64;
    private static final byte VAR_I_BOOL = VAR_I | TYPE_BOOL;
    private static final byte VAR_I_FLOAT = VAR_I | TYPE_FLOAT;
    private static final byte VAR_I_DOUBLE = VAR_I | TYPE_DOUBLE;
    private static final byte VAR_I_DECIMAL = VAR_I | TYPE_DECIMAL;
    private static final byte VAR_I_STRING = VAR_I | TYPE_STRING;

    private static final byte POS = (byte) 0x81;
    private static final byte NEG = (byte) 0x82;
    private static final byte ADD = (byte) 0x83;
    private static final byte SUB = (byte) 0x84;
    private static final byte MUL = (byte) 0x85;
    private static final byte DIV = (byte) 0x86;
    private static final byte MOD = (byte) 0x87;

    private static final byte EQ = (byte) 0x91;
    private static final byte GE = (byte) 0x92;
    private static final byte GT = (byte) 0x93;
    private static final byte LE = (byte) 0x94;
    private static final byte LT = (byte) 0x95;
    private static final byte NE = (byte) 0x96;

    private static final byte NOT = (byte) 0x51;
    private static final byte AND = (byte) 0x52;
    private static final byte OR = (byte) 0x53;

    private static final byte IS_NULL = (byte) 0xA1;
    private static final byte IS_TRUE = (byte) 0xA2;
    private static final byte IS_FALSE = (byte) 0xA3;

    private static final byte CAST = (byte) 0xF0;

    private final OutputStream os;

    @SneakyThrows(IOException.class)
    private boolean visitUnaryEval(@NonNull UnaryEval eval, byte... opBytes) {
        if (eval.getOperand().accept(this)) {
            os.write(opBytes);
            return true;
        }
        return false;
    }

    @SneakyThrows(IOException.class)
    private boolean visitBinaryEval(@NonNull BinaryEval eval, byte... opBytes) {
        if (eval.getOperand0().accept(this) && eval.getOperand1().accept(this)) {
            os.write(opBytes);
            return true;
        }
        return false;
    }

    @SneakyThrows(IOException.class)
    private boolean cascadingBinaryLogical(Eval @NonNull [] operands, byte opByte) {
        if (!operands[0].accept(this)) {
            return false;
        }
        for (int i = 1; i < operands.length; ++i) {
            if (!operands[i].accept(this)) {
                return false;
            }
            os.write(opByte);
        }
        return true;
    }

    @SneakyThrows(IOException.class)
    @Override
    public Boolean visit(@NonNull IntValue eval) {
        Integer value = eval.getValue();
        if (value != null) {
            if (value >= 0) {
                os.write(CONST_INT32);
                CodecUtils.encodeVarInt(os, value);
            } else {
                os.write(CONST_N_INT32);
                CodecUtils.encodeVarInt(os, -value);
            }
        } else {
            os.write(NULL_INT32);
        }
        return true;
    }

    @SneakyThrows(IOException.class)
    @Override
    public Boolean visit(@NonNull LongValue eval) {
        Long value = eval.getValue();
        if (value != null) {
            if (value >= 0) {
                os.write(CONST_INT64);
                CodecUtils.encodeVarInt(os, value);
            } else {
                os.write(CONST_N_INT64);
                CodecUtils.encodeVarInt(os, -value);
            }
        } else {
            os.write(NULL_INT64);
        }
        return true;
    }

    @SneakyThrows(IOException.class)
    @Override
    public Boolean visit(@NonNull FloatValue eval) {
        Float value = eval.getValue();
        if (value != null) {
            os.write(CONST_FLOAT);
            CodecUtils.encodeFloat(os, value);
        } else {
            os.write(NULL_FLOAT);
        }
        return true;
    }

    @SneakyThrows(IOException.class)
    @Override
    public Boolean visit(@NonNull DoubleValue eval) {
        Double value = eval.getValue();
        if (value != null) {
            os.write(CONST_DOUBLE);
            CodecUtils.encodeDouble(os, value);
        } else {
            os.write(NULL_DOUBLE);
        }
        return true;
    }

    @SneakyThrows(IOException.class)
    @Override
    public Boolean visit(@NonNull BoolValue eval) {
        Boolean value = eval.getValue();
        if (value != null) {
            os.write(value ? CONST_BOOL : CONST_N_BOOL);
        } else {
            os.write(NULL_BOOL);
        }
        return true;
    }

    @SneakyThrows(IOException.class)
    @Override
    public Boolean visit(@NonNull StringValue eval) {
        String value = eval.getValue();
        if (value != null) {
            os.write(CONST_STRING);
            byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
            CodecUtils.encodeVarInt(os, bytes.length);
            os.write(bytes);
        } else {
            os.write(NULL_STRING);
        }
        return true;
    }

    @Override
    public Boolean visit(@NonNull LongToInt eval) {
        return visitUnaryEval(eval, CAST, (byte) (TYPE_INT32 << 4 | TYPE_INT64));
    }

    @Override
    public Boolean visit(@NonNull FloatToInt eval) {
        return visitUnaryEval(eval, CAST, (byte) (TYPE_INT32 << 4 | TYPE_FLOAT));
    }

    @Override
    public Boolean visit(@NonNull DoubleToInt eval) {
        return visitUnaryEval(eval, CAST, (byte) (TYPE_INT32 << 4 | TYPE_DOUBLE));
    }

    @Override
    public Boolean visit(@NonNull IntToLong eval) {
        return visitUnaryEval(eval, CAST, (byte) (TYPE_INT64 << 4 | TYPE_INT32));
    }

    @Override
    public Boolean visit(@NonNull FloatToLong eval) {
        return visitUnaryEval(eval, CAST, (byte) (TYPE_INT64 << 4 | TYPE_FLOAT));
    }

    @Override
    public Boolean visit(@NonNull DoubleToLong eval) {
        return visitUnaryEval(eval, CAST, (byte) (TYPE_INT64 << 4 | TYPE_DOUBLE));
    }

    @Override
    public Boolean visit(@NonNull IntToFloat eval) {
        return visitUnaryEval(eval, CAST, (byte) (TYPE_FLOAT << 4 | TYPE_INT32));
    }

    @Override
    public Boolean visit(@NonNull LongToFloat eval) {
        return visitUnaryEval(eval, CAST, (byte) (TYPE_FLOAT << 4 | TYPE_INT64));
    }

    @Override
    public Boolean visit(@NonNull DoubleToFloat eval) {
        return visitUnaryEval(eval, CAST, (byte) (TYPE_FLOAT << 4 | TYPE_DOUBLE));
    }

    @Override
    public Boolean visit(@NonNull IntToDouble eval) {
        return visitUnaryEval(eval, CAST, (byte) (TYPE_DOUBLE << 4 | TYPE_INT32));
    }

    @Override
    public Boolean visit(@NonNull LongToDouble eval) {
        return visitUnaryEval(eval, CAST, (byte) (TYPE_DOUBLE << 4 | TYPE_INT64));
    }

    @Override
    public Boolean visit(@NonNull FloatToDouble eval) {
        return visitUnaryEval(eval, CAST, (byte) (TYPE_DOUBLE << 4 | TYPE_FLOAT));
    }

    @Override
    public Boolean visit(@NonNull IntToBool eval) {
        return visitUnaryEval(eval, CAST, (byte) (TYPE_BOOL << 4 | TYPE_INT32));
    }

    @Override
    public Boolean visit(@NonNull LongToBool eval) {
        return visitUnaryEval(eval, CAST, (byte) (TYPE_BOOL << 4 | TYPE_INT64));
    }

    @Override
    public Boolean visit(@NonNull FloatToBool eval) {
        return visitUnaryEval(eval, CAST, (byte) (TYPE_BOOL << 4 | TYPE_FLOAT));
    }

    @Override
    public Boolean visit(@NonNull DoubleToBool eval) {
        return visitUnaryEval(eval, CAST, (byte) (TYPE_BOOL << 4 | TYPE_DOUBLE));
    }

    @Override
    public Boolean visit(@NonNull PosInt eval) {
        return visitUnaryEval(eval, POS, TYPE_INT32);
    }

    @Override
    public Boolean visit(@NonNull PosLong eval) {
        return visitUnaryEval(eval, POS, TYPE_INT64);
    }

    @Override
    public Boolean visit(@NonNull PosFloat eval) {
        return visitUnaryEval(eval, POS, TYPE_FLOAT);
    }

    @Override
    public Boolean visit(@NonNull PosDouble eval) {
        return visitUnaryEval(eval, POS, TYPE_DOUBLE);
    }

    @Override
    public Boolean visit(@NonNull NegInt eval) {
        return visitUnaryEval(eval, NEG, TYPE_INT32);
    }

    @Override
    public Boolean visit(@NonNull NegLong eval) {
        return visitUnaryEval(eval, NEG, TYPE_INT64);
    }

    @Override
    public Boolean visit(@NonNull NegFloat eval) {
        return visitUnaryEval(eval, NEG, TYPE_FLOAT);
    }

    @Override
    public Boolean visit(@NonNull NegDouble eval) {
        return visitUnaryEval(eval, NEG, TYPE_DOUBLE);
    }

    @Override
    public Boolean visit(@NonNull AddInt eval) {
        return visitBinaryEval(eval, ADD, TYPE_INT32);
    }

    @Override
    public Boolean visit(@NonNull AddLong eval) {
        return visitBinaryEval(eval, ADD, TYPE_INT64);
    }

    @Override
    public Boolean visit(@NonNull AddFloat eval) {
        return visitBinaryEval(eval, ADD, TYPE_FLOAT);
    }

    @Override
    public Boolean visit(@NonNull AddDouble eval) {
        return visitBinaryEval(eval, ADD, TYPE_DOUBLE);
    }

    @Override
    public Boolean visit(@NonNull SubInt eval) {
        return visitBinaryEval(eval, SUB, TYPE_INT32);
    }

    @Override
    public Boolean visit(@NonNull SubLong eval) {
        return visitBinaryEval(eval, SUB, TYPE_INT64);
    }

    @Override
    public Boolean visit(@NonNull SubFloat eval) {
        return visitBinaryEval(eval, SUB, TYPE_FLOAT);
    }

    @Override
    public Boolean visit(@NonNull SubDouble eval) {
        return visitBinaryEval(eval, SUB, TYPE_DOUBLE);
    }

    @Override
    public Boolean visit(@NonNull MulInt eval) {
        return visitBinaryEval(eval, MUL, TYPE_INT32);
    }

    @Override
    public Boolean visit(@NonNull MulLong eval) {
        return visitBinaryEval(eval, MUL, TYPE_INT64);
    }

    @Override
    public Boolean visit(@NonNull MulFloat eval) {
        return visitBinaryEval(eval, MUL, TYPE_FLOAT);
    }

    @Override
    public Boolean visit(@NonNull MulDouble eval) {
        return visitBinaryEval(eval, MUL, TYPE_DOUBLE);
    }

    @Override
    public Boolean visit(@NonNull DivInt eval) {
        return visitBinaryEval(eval, DIV, TYPE_INT32);
    }

    @Override
    public Boolean visit(@NonNull DivLong eval) {
        return visitBinaryEval(eval, DIV, TYPE_INT64);
    }

    @Override
    public Boolean visit(@NonNull DivFloat eval) {
        return visitBinaryEval(eval, DIV, TYPE_FLOAT);
    }

    @Override
    public Boolean visit(@NonNull DivDouble eval) {
        return visitBinaryEval(eval, DIV, TYPE_DOUBLE);
    }

    @Override
    public Boolean visit(@NonNull EqInt eval) {
        return visitBinaryEval(eval, EQ, TYPE_INT32);
    }

    @Override
    public Boolean visit(@NonNull EqLong eval) {
        return visitBinaryEval(eval, EQ, TYPE_INT64);
    }

    @Override
    public Boolean visit(@NonNull EqFloat eval) {
        return visitBinaryEval(eval, EQ, TYPE_FLOAT);
    }

    @Override
    public Boolean visit(@NonNull EqDouble eval) {
        return visitBinaryEval(eval, EQ, TYPE_DOUBLE);
    }

    @Override
    public Boolean visit(@NonNull EqBool eval) {
        return visitBinaryEval(eval, EQ, TYPE_BOOL);
    }

    @Override
    public Boolean visit(@NonNull EqString eval) {
        return visitBinaryEval(eval, EQ, TYPE_STRING);
    }

    @Override
    public Boolean visit(@NonNull GeInt eval) {
        return visitBinaryEval(eval, GE, TYPE_INT32);
    }

    @Override
    public Boolean visit(@NonNull GeLong eval) {
        return visitBinaryEval(eval, GE, TYPE_INT64);
    }

    @Override
    public Boolean visit(@NonNull GeFloat eval) {
        return visitBinaryEval(eval, GE, TYPE_FLOAT);
    }

    @Override
    public Boolean visit(@NonNull GeDouble eval) {
        return visitBinaryEval(eval, GE, TYPE_DOUBLE);
    }

    @Override
    public Boolean visit(@NonNull GeBool eval) {
        return visitBinaryEval(eval, GE, TYPE_BOOL);
    }

    @Override
    public Boolean visit(@NonNull GeString eval) {
        return visitBinaryEval(eval, GE, TYPE_STRING);
    }

    @Override
    public Boolean visit(@NonNull GtInt eval) {
        return visitBinaryEval(eval, GT, TYPE_INT32);
    }

    @Override
    public Boolean visit(@NonNull GtLong eval) {
        return visitBinaryEval(eval, GT, TYPE_INT64);
    }

    @Override
    public Boolean visit(@NonNull GtFloat eval) {
        return visitBinaryEval(eval, GT, TYPE_FLOAT);
    }

    @Override
    public Boolean visit(@NonNull GtDouble eval) {
        return visitBinaryEval(eval, GT, TYPE_DOUBLE);
    }

    @Override
    public Boolean visit(@NonNull GtBool eval) {
        return visitBinaryEval(eval, GT, TYPE_BOOL);
    }

    @Override
    public Boolean visit(@NonNull GtString eval) {
        return visitBinaryEval(eval, GT, TYPE_STRING);
    }

    @Override
    public Boolean visit(@NonNull LeInt eval) {
        return visitBinaryEval(eval, LE, TYPE_INT32);
    }

    @Override
    public Boolean visit(@NonNull LeLong eval) {
        return visitBinaryEval(eval, LE, TYPE_INT64);
    }

    @Override
    public Boolean visit(@NonNull LeFloat eval) {
        return visitBinaryEval(eval, LE, TYPE_FLOAT);
    }

    @Override
    public Boolean visit(@NonNull LeDouble eval) {
        return visitBinaryEval(eval, LE, TYPE_DOUBLE);
    }

    @Override
    public Boolean visit(@NonNull LeBool eval) {
        return visitBinaryEval(eval, LE, TYPE_BOOL);
    }

    @Override
    public Boolean visit(@NonNull LeString eval) {
        return visitBinaryEval(eval, LE, TYPE_STRING);
    }

    @Override
    public Boolean visit(@NonNull LtInt eval) {
        return visitBinaryEval(eval, LT, TYPE_INT32);
    }

    @Override
    public Boolean visit(@NonNull LtLong eval) {
        return visitBinaryEval(eval, LT, TYPE_INT64);
    }

    @Override
    public Boolean visit(@NonNull LtFloat eval) {
        return visitBinaryEval(eval, LT, TYPE_FLOAT);
    }

    @Override
    public Boolean visit(@NonNull LtDouble eval) {
        return visitBinaryEval(eval, LT, TYPE_DOUBLE);
    }

    @Override
    public Boolean visit(@NonNull LtBool eval) {
        return visitBinaryEval(eval, LT, TYPE_BOOL);
    }

    @Override
    public Boolean visit(@NonNull LtString eval) {
        return visitBinaryEval(eval, LT, TYPE_STRING);
    }

    @Override
    public Boolean visit(@NonNull NeInt eval) {
        return visitBinaryEval(eval, NE, TYPE_INT32);
    }

    @Override
    public Boolean visit(@NonNull NeLong eval) {
        return visitBinaryEval(eval, NE, TYPE_INT64);
    }

    @Override
    public Boolean visit(@NonNull NeFloat eval) {
        return visitBinaryEval(eval, NE, TYPE_FLOAT);
    }

    @Override
    public Boolean visit(@NonNull NeDouble eval) {
        return visitBinaryEval(eval, NE, TYPE_DOUBLE);
    }

    @Override
    public Boolean visit(@NonNull NeBool eval) {
        return visitBinaryEval(eval, NE, TYPE_BOOL);
    }

    @Override
    public Boolean visit(@NonNull NeString eval) {
        return visitBinaryEval(eval, NE, TYPE_STRING);
    }

    @Override
    public Boolean visit(@NonNull IsNullInt eval) {
        return visitUnaryEval(eval, IS_NULL, TYPE_INT32);
    }

    @Override
    public Boolean visit(@NonNull IsNullLong eval) {
        return visitUnaryEval(eval, IS_NULL, TYPE_INT64);
    }

    @Override
    public Boolean visit(@NonNull IsNullFloat eval) {
        return visitUnaryEval(eval, IS_NULL, TYPE_FLOAT);
    }

    @Override
    public Boolean visit(@NonNull IsNullDouble eval) {
        return visitUnaryEval(eval, IS_NULL, TYPE_DOUBLE);
    }

    @Override
    public Boolean visit(@NonNull IsNullBool eval) {
        return visitUnaryEval(eval, IS_NULL, TYPE_BOOL);
    }

    @Override
    public Boolean visit(@NonNull IsNullString eval) {
        return visitUnaryEval(eval, IS_NULL, TYPE_STRING);
    }

    @Override
    public Boolean visit(@NonNull IsTrueInt eval) {
        return visitUnaryEval(eval, IS_TRUE, TYPE_INT32);
    }

    @Override
    public Boolean visit(@NonNull IsTrueLong eval) {
        return visitUnaryEval(eval, IS_TRUE, TYPE_INT64);
    }

    @Override
    public Boolean visit(@NonNull IsTrueFloat eval) {
        return visitUnaryEval(eval, IS_TRUE, TYPE_FLOAT);
    }

    @Override
    public Boolean visit(@NonNull IsTrueDouble eval) {
        return visitUnaryEval(eval, IS_TRUE, TYPE_DOUBLE);
    }

    @Override
    public Boolean visit(@NonNull IsTrueBool eval) {
        return visitUnaryEval(eval, IS_TRUE, TYPE_BOOL);
    }

    @Override
    public Boolean visit(@NonNull IsTrueString eval) {
        return visitUnaryEval(eval, IS_TRUE, TYPE_STRING);
    }

    @Override
    public Boolean visit(@NonNull IsFalseInt eval) {
        return visitUnaryEval(eval, IS_FALSE, TYPE_INT32);
    }

    @Override
    public Boolean visit(@NonNull IsFalseLong eval) {
        return visitUnaryEval(eval, IS_FALSE, TYPE_INT64);
    }

    @Override
    public Boolean visit(@NonNull IsFalseFloat eval) {
        return visitUnaryEval(eval, IS_FALSE, TYPE_FLOAT);
    }

    @Override
    public Boolean visit(@NonNull IsFalseDouble eval) {
        return visitUnaryEval(eval, IS_FALSE, TYPE_DOUBLE);
    }

    @Override
    public Boolean visit(@NonNull IsFalseBool eval) {
        return visitUnaryEval(eval, IS_FALSE, TYPE_BOOL);
    }

    @Override
    public Boolean visit(@NonNull IsFalseString eval) {
        return visitUnaryEval(eval, IS_FALSE, TYPE_STRING);
    }

    @Override
    public Boolean visit(@NonNull NotEval eval) {
        return visitUnaryEval(eval, NOT);
    }

    @Override
    public Boolean visit(@NonNull AndEval eval) {
        return visitBinaryEval(eval, AND);
    }

    @Override
    public Boolean visit(@NonNull OrEval eval) {
        return visitBinaryEval(eval, OR);
    }

    @Override
    public Boolean visit(@NonNull VarArgAndEval eval) {
        // TODO: wait for vararg support of store.
        return cascadingBinaryLogical(eval.getOperands(), AND);
    }

    @Override
    public Boolean visit(@NonNull VarArgOrEval eval) {
        // TODO: wait for vararg support of store.
        return cascadingBinaryLogical(eval.getOperands(), OR);
    }

    @SneakyThrows(IOException.class)
    @Override
    public Boolean visit(@NonNull IndexedVar eval) {
        switch (eval.getType()) {
            case TypeCode.INT:
                os.write(VAR_I_INT32);
                break;
            case TypeCode.LONG:
                os.write(VAR_I_INT64);
                break;
            case TypeCode.BOOL:
                os.write(VAR_I_BOOL);
                break;
            case TypeCode.FLOAT:
                os.write(VAR_I_FLOAT);
                break;
            case TypeCode.DOUBLE:
                os.write(VAR_I_DOUBLE);
                break;
            case TypeCode.DECIMAL:
                throw new TemporarilyUnsupported("Decimal Type");
            case TypeCode.STRING:
                os.write(VAR_I_STRING);
                break;
            default:
                return null;
        }
        int index = eval.getIndex();
        if (index >= 0) {
            CodecUtils.encodeVarInt(os, eval.getIndex());
            return true;
        }
        // TODO: SQL parameters are not supported currently.
        return false;
    }

    @Override
    public Boolean visit(@NonNull NamedVar eval) {
        throw new TemporarilyUnsupported("Named Var");
    }
}
