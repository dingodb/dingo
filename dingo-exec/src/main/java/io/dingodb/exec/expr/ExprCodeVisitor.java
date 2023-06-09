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
import io.dingodb.expr.parser.Expr;
import io.dingodb.expr.parser.ExprVisitor;
import io.dingodb.expr.parser.exception.ExprCompileException;
import io.dingodb.expr.parser.op.AndOp;
import io.dingodb.expr.parser.op.IsFalseOp;
import io.dingodb.expr.parser.op.IsNotFalseOp;
import io.dingodb.expr.parser.op.IsNotNullOp;
import io.dingodb.expr.parser.op.IsNotTrueOp;
import io.dingodb.expr.parser.op.IsNullOp;
import io.dingodb.expr.parser.op.IsTrueOp;
import io.dingodb.expr.parser.op.Op;
import io.dingodb.expr.parser.op.OpType;
import io.dingodb.expr.parser.op.OrOp;
import io.dingodb.expr.parser.value.Null;
import io.dingodb.expr.parser.value.Value;
import io.dingodb.expr.parser.var.Var;
import io.dingodb.expr.runtime.CompileContext;
import io.dingodb.expr.runtime.EvalContext;
import io.dingodb.expr.runtime.var.RtVar;
import lombok.SneakyThrows;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Objects;

public class ExprCodeVisitor implements ExprVisitor<ExprCodeType> {
    private static final byte TYPE_INT32 = (byte) 0x01;
    private static final byte TYPE_INT64 = (byte) 0x02;
    private static final byte TYPE_BOOL = (byte) 0x03;
    private static final byte TYPE_FLOAT = (byte) 0x04;
    private static final byte TYPE_DOUBLE = (byte) 0x05;
    private static final byte TYPE_DECIMAL = (byte) 0x06;
    private static final byte TYPE_STRING = (byte) 0x07;

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

    private final CompileContext ctx;
    private final EvalContext etx;

    public ExprCodeVisitor(@Nullable CompileContext ctx, @Nullable EvalContext etx) {
        this.ctx = ctx;
        this.etx = etx;
    }

    private static byte codingType(int type) {
        switch (type) {
            case TypeCode.INT:
                return TYPE_INT32;
            case TypeCode.LONG:
                return TYPE_INT64;
            case TypeCode.BOOL:
                return TYPE_BOOL;
            case TypeCode.FLOAT:
                return TYPE_FLOAT;
            case TypeCode.DOUBLE:
                return TYPE_DOUBLE;
            case TypeCode.DECIMAL:
                return TYPE_DECIMAL;
            case TypeCode.STRING:
                return TYPE_STRING;
        }
        return (byte) 0;
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

    private static void writeOperandWithCasting(
        @NonNull OutputStream buf,
        @NonNull ExprCodeType operandCodeType,
        int targetType
    ) throws IOException {
        buf.write(operandCodeType.getCode());
        int type = operandCodeType.getType();
        if (type != targetType) {
            buf.write(CAST);
            buf.write(codingType(targetType) << 4 | codingType(type));
        }
    }

    @SneakyThrows(IOException.class)
    private static @Nullable ExprCodeType codingConst(int typeCode, @NonNull Object value) {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        switch (typeCode) {
            case TypeCode.INT:
                if (((Integer) value) >= 0) {
                    buf.write(CONST_INT32);
                    CodecUtils.encodeVarInt(buf, (Integer) value);
                } else {
                    buf.write(CONST_N_INT32);
                    CodecUtils.encodeVarInt(buf, -(Integer) value);
                }
                break;
            case TypeCode.LONG:
                if (((Long) value) >= 0) {
                    buf.write(CONST_INT64);
                    CodecUtils.encodeVarInt(buf, (Long) value);
                } else {
                    buf.write(CONST_N_INT64);
                    CodecUtils.encodeVarInt(buf, -(Long) value);
                }
                break;
            case TypeCode.BOOL:
                buf.write(((Boolean) value) ? CONST_BOOL : CONST_N_BOOL);
                break;
            case TypeCode.FLOAT:
                buf.write(CONST_FLOAT);
                CodecUtils.encodeFloat(buf, (Float) value);
                break;
            case TypeCode.DOUBLE:
                buf.write(CONST_DOUBLE);
                CodecUtils.encodeDouble(buf, (Double) value);
                break;
            default:
                return null;
        }
        return new ExprCodeType(typeCode, buf.toByteArray());
    }

    @SneakyThrows(IOException.class)
    private static @NonNull ExprCodeType codingUnaryLogicalOperator(
        byte code,
        @NonNull ExprCodeType operandCodeType
    ) {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        writeOperandWithCasting(buf, operandCodeType, TypeCode.BOOL);
        buf.write(code);
        return new ExprCodeType(TypeCode.BOOL, buf.toByteArray());
    }

    @SneakyThrows(IOException.class)
    private static @NonNull ExprCodeType codingBinaryLogicalOperator(
        byte code,
        @NonNull ExprCodeType operandCodeType0,
        @NonNull ExprCodeType operandCodeType1
    ) {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        writeOperandWithCasting(buf, operandCodeType0, TypeCode.BOOL);
        writeOperandWithCasting(buf, operandCodeType1, TypeCode.BOOL);
        buf.write(code);
        return new ExprCodeType(TypeCode.BOOL, buf.toByteArray());
    }

    @SneakyThrows(IOException.class)
    private static @NonNull ExprCodeType codingUnaryArithmeticOperator(
        byte code,
        @NonNull ExprCodeType operandCodeType
    ) {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        buf.write(operandCodeType.getCode());
        buf.write(code);
        return new ExprCodeType(operandCodeType.getType(), buf.toByteArray());
    }

    @SneakyThrows(IOException.class)
    private static @NonNull ExprCodeType codingBinaryArithmeticOperator(
        byte code,
        @NonNull ExprCodeType @NonNull [] operandCodeTypes
    ) {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        int type0 = operandCodeTypes[0].getType();
        int type1 = operandCodeTypes[1].getType();
        int targetType = bestNumericalType(type0, type1);
        if (targetType == -1) {
            throw new RuntimeException("Numerical types required");
        }
        writeOperandWithCasting(buf, operandCodeTypes[0], targetType);
        writeOperandWithCasting(buf, operandCodeTypes[1], targetType);
        buf.write(code);
        buf.write(codingType(targetType));
        return new ExprCodeType(targetType, buf.toByteArray());
    }

    @SneakyThrows(IOException.class)
    private static @NonNull ExprCodeType codingRelationalOperator(
        byte code,
        @NonNull ExprCodeType @NonNull [] operandCodeTypes
    ) {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        int type0 = operandCodeTypes[0].getType();
        int type1 = operandCodeTypes[1].getType();
        int targetType = bestNumericalType(type0, type1);
        if (targetType == -1) {
            if (type0 == TypeCode.STRING && type1 == TypeCode.STRING) {
                targetType = TypeCode.STRING;
            } else if (type0 == TypeCode.BOOL && type1 == TypeCode.BOOL) {
                targetType = TypeCode.BOOL;
            } else {
                throw new RuntimeException("Type mismatch.");
            }
        }
        writeOperandWithCasting(buf, operandCodeTypes[0], targetType);
        writeOperandWithCasting(buf, operandCodeTypes[1], targetType);
        buf.write(code);
        buf.write(codingType(targetType));
        return new ExprCodeType(TypeCode.BOOL, buf.toByteArray());
    }

    @SneakyThrows(IOException.class)
    private static @NonNull ExprCodeType codingSpecialFun(
        byte code,
        @NonNull ExprCodeType operandCodeType,
        boolean withNot
    ) {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        buf.write(operandCodeType.getCode());
        buf.write(code);
        buf.write(codingType(operandCodeType.getType()));
        if (withNot) {
            buf.write(NOT);
        }
        return new ExprCodeType(TypeCode.BOOL, buf.toByteArray());
    }

    @SneakyThrows(IOException.class)
    private static @NonNull ExprCodeType codingCastFun(
        int targetType,
        @NonNull ExprCodeType operandCodeType
    ) {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        writeOperandWithCasting(buf, operandCodeType, targetType);
        return new ExprCodeType(targetType, buf.toByteArray());
    }

    @SneakyThrows(IOException.class)
    private static @Nullable ExprCodeType codingTupleVar(int type, int index) {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        switch (type) {
            case TypeCode.INT:
                buf.write(VAR_I_INT32);
                break;
            case TypeCode.LONG:
                buf.write(VAR_I_INT64);
                break;
            case TypeCode.BOOL:
                buf.write(VAR_I_BOOL);
                break;
            case TypeCode.FLOAT:
                buf.write(VAR_I_FLOAT);
                break;
            case TypeCode.DOUBLE:
                buf.write(VAR_I_DOUBLE);
                break;
            case TypeCode.DECIMAL:
                buf.write(VAR_I_DECIMAL);
                break;
            case TypeCode.STRING:
                // TODO: dingo-store does not support string op.
                return null;
            default:
                return null;
        }
        CodecUtils.encodeVarInt(buf, index);
        return new ExprCodeType(type, buf.toByteArray());
    }

    private static ExprCodeType cascadeCodingLogicalOperator(byte code, ExprCodeType @NonNull [] exprCodeTypes) {
        ExprCodeType exprCodeType = exprCodeTypes[0];
        for (int i = 1; i < exprCodeTypes.length; ++i) {
            exprCodeType = codingBinaryLogicalOperator(code, exprCodeType, exprCodeTypes[i]);
        }
        return exprCodeType;
    }

    @Override
    public ExprCodeType visit(@NonNull Null op) {
        return null;
    }

    @Override
    public <V> ExprCodeType visit(@NonNull Value<V> op) {
        V value = op.getValue();
        int typeCode = TypeCode.getTypeCode(value);
        return codingConst(typeCode, value);
    }

    @SneakyThrows({ExprCompileException.class})
    @Override
    public ExprCodeType visit(@NonNull Op op) {
        if (op.getType() == OpType.INDEX) {
            Expr[] exprArray = op.getExprArray();
            if (exprArray[0] instanceof Var) {
                Var var = (Var) exprArray[0];
                if (SqlExprCompileContext.TUPLE_VAR_NAME.equals(var.getName())) {
                    RtVar rtVar = (RtVar) op.compileIn(ctx);
                    return codingTupleVar(rtVar.typeCode(), (int) rtVar.getId());
                } else if (SqlExprCompileContext.SQL_DYNAMIC_VAR_NAME.equals(var.getName())) {
                    RtVar rtVar = (RtVar) op.compileIn(ctx);
                    Object value = rtVar.eval(etx);
                    return codingConst(rtVar.typeCode(), value);
                }
            }
        }
        ExprCodeType[] exprCodeTypes = Arrays.stream(op.getExprArray())
            .map(expr -> expr.accept(this))
            .toArray(ExprCodeType[]::new);
        if (Arrays.stream(exprCodeTypes).anyMatch(Objects::isNull)) {
            return null;
        }
        switch (op.getType()) {
            case NOT:
                return codingUnaryLogicalOperator(NOT, exprCodeTypes[0]);
            case AND:
                return codingBinaryLogicalOperator(AND, exprCodeTypes[0], exprCodeTypes[1]);
            case OR:
                return codingBinaryLogicalOperator(OR, exprCodeTypes[0], exprCodeTypes[1]);
            case POS:
                return codingUnaryArithmeticOperator(POS, exprCodeTypes[0]);
            case NEG:
                return codingUnaryArithmeticOperator(NEG, exprCodeTypes[0]);
            case ADD:
                return codingBinaryArithmeticOperator(ADD, exprCodeTypes);
            case SUB:
                return codingBinaryArithmeticOperator(SUB, exprCodeTypes);
            case MUL:
                return codingBinaryArithmeticOperator(MUL, exprCodeTypes);
            case DIV:
                return codingBinaryArithmeticOperator(DIV, exprCodeTypes);
            case EQ:
                return codingRelationalOperator(EQ, exprCodeTypes);
            case GE:
                return codingRelationalOperator(GE, exprCodeTypes);
            case GT:
                return codingRelationalOperator(GT, exprCodeTypes);
            case LE:
                return codingRelationalOperator(LE, exprCodeTypes);
            case LT:
                return codingRelationalOperator(LT, exprCodeTypes);
            case NE:
                return codingRelationalOperator(NE, exprCodeTypes);
            case FUN:
                switch (op.getName()) {
                    case AndOp.FUN_NAME:
                        return cascadeCodingLogicalOperator(AND, exprCodeTypes);
                    case OrOp.FUN_NAME:
                        return cascadeCodingLogicalOperator(OR, exprCodeTypes);
                    case IsNullOp.FUN_NAME:
                        return codingSpecialFun(IS_NULL, exprCodeTypes[0], false);
                    case IsNotNullOp.FUN_NAME:
                        return codingSpecialFun(IS_NULL, exprCodeTypes[0], true);
                    case IsTrueOp.FUN_NAME:
                        return codingSpecialFun(IS_TRUE, exprCodeTypes[0], false);
                    case IsNotTrueOp.FUN_NAME:
                        return codingSpecialFun(IS_TRUE, exprCodeTypes[0], true);
                    case IsFalseOp.FUN_NAME:
                        return codingSpecialFun(IS_FALSE, exprCodeTypes[0], false);
                    case IsNotFalseOp.FUN_NAME:
                        return codingSpecialFun(IS_FALSE, exprCodeTypes[0], true);
                    case TypeCode.INT_NAME:
                        return codingCastFun(TypeCode.INT, exprCodeTypes[0]);
                    case TypeCode.LONG_NAME:
                        return codingCastFun(TypeCode.LONG, exprCodeTypes[0]);
                    case TypeCode.BOOL_NAME:
                        return codingCastFun(TypeCode.BOOL, exprCodeTypes[0]);
                    case TypeCode.FLOAT_NAME:
                        return codingCastFun(TypeCode.FLOAT, exprCodeTypes[0]);
                    case TypeCode.DOUBLE_NAME:
                        return codingCastFun(TypeCode.DOUBLE, exprCodeTypes[0]);
                    case TypeCode.DECIMAL_NAME:
                        return codingCastFun(TypeCode.DECIMAL, exprCodeTypes[0]);
                    case TypeCode.STRING_NAME:
                        return codingCastFun(TypeCode.STRING, exprCodeTypes[0]);
                    default:
                        break;
                }
            default:
                break;
        }
        return null;
    }

    @Override
    public ExprCodeType visit(@NonNull Var op) {
        return null;
    }
}
