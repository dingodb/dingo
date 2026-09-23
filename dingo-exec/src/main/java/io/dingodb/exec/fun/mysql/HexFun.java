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

package io.dingodb.exec.fun.mysql;

import io.dingodb.expr.common.type.Type;
import io.dingodb.expr.common.type.Types;
import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.expr.runtime.op.BinaryOp;
import io.dingodb.expr.runtime.op.UnaryOp;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.Serial;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class HexFun extends UnaryOp {
    public static final String NAME = "hex";
    public static final String CHARSET_NAME = "hex_charset";
    public static final BinaryOp CHARSET_INSTANCE = new CharsetHexFun();
    @Serial
    private static final long serialVersionUID = -2489040936115125799L;
    private static final char[] HEX_DIGITS = "0123456789ABCDEF".toCharArray();

    public static final HexFun INSTANCE = new HexFun();

    @Override
    protected Object evalNonNullValue(@NonNull Object value, ExprConfig config) {
        return hex(value, StandardCharsets.UTF_8);
    }

    private static Object hex(@NonNull Object value, Charset charset) {
        if (value instanceof byte[]) {
            return toHex((byte[]) value);
        }
        if (value instanceof Double) {
            return Double.toHexString((Double) value).toUpperCase();
        } else if (value instanceof Float) {
            return Float.toHexString((Float) value).toUpperCase();
        } else if (value instanceof BigDecimal) {
            BigDecimal valueDecimal = (BigDecimal) value;
            return valueDecimal.toBigInteger().toString(16).toUpperCase();
        } else if (value instanceof Integer) {
            Integer valInt = (Integer) value;
            return Long.toHexString(valInt.longValue()).toUpperCase();
        } else if (value instanceof Long) {
            return Long.toHexString((Long) value).toUpperCase();
        } else {
            return toHex(value.toString().getBytes(charset));
        }
    }

    private static final class CharsetHexFun extends BinaryOp {
        @Serial
        private static final long serialVersionUID = 4669681289107118463L;

        @Override
        public Object evalValue(Object value, Object charsetName, ExprConfig config) {
            return value == null ? null : hex(value, Charset.forName(charsetName.toString()));
        }

        @Override
        public @NonNull String getName() {
            return CHARSET_NAME;
        }

        @Override
        public Type getType() {
            return Types.STRING;
        }
    }

    @Override
    public @NonNull String getName() {
        return NAME;
    }

    public static String toHex(byte[] bytes) {
        if (bytes == null) {
            return "null";
        }
        char[] result = new char[bytes.length * 2];
        for (int i = 0; i < bytes.length; ++i) {
            int value = bytes[i] & 0xff;
            result[2 * i] = HEX_DIGITS[value >>> 4];
            result[2 * i + 1] = HEX_DIGITS[value & 0xf];
        }
        return new String(result);
    }

    @Override
    public Type getType() {
        return Types.STRING;
    }
}
