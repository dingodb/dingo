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
import io.dingodb.expr.runtime.op.OpKey;
import io.dingodb.expr.runtime.op.OpKeys;
import io.dingodb.expr.runtime.op.VariadicOp;
import org.checkerframework.checker.nullness.qual.NonNull;

/** MySQL CHAR() constructs a binary string from integer byte values. */
public class CharFun extends VariadicOp {
    public static final String NAME = "char";
    @SuppressWarnings("serial")
    private static final long serialVersionUID = -5487904391104558091L;

    public static final CharFun INSTANCE = new CharFun();

    @Override
    public Object evalValue(Object @NonNull [] values, ExprConfig config) {
        return buildBytes(values, values.length);
    }

    /** Each integer contributes its nonzero leading bytes, most significant first. */
    static byte[] buildBytes(Object @NonNull [] values, int count) {
        byte[] bytes = new byte[Math.max(count, 4)];
        int length = 0;
        for (int i = 0; i < count; i++) {
            Object value = values[i];
            byte[] valueBytes;
            if (value instanceof Number || value instanceof String) {
                long number = value instanceof Number
                    ? ((Number) value).longValue() : Long.parseLong(((String) value).trim());
                int width = 1;
                for (long remaining = number >>> 8; remaining != 0; remaining >>>= 8) {
                    width++;
                }
                int required = length + width;
                if (required > bytes.length) {
                    byte[] grown = new byte[Math.max(required, bytes.length * 2)];
                    System.arraycopy(bytes, 0, grown, 0, length);
                    bytes = grown;
                }
                for (int shift = (width - 1) * 8; shift >= 0; shift -= 8) {
                    bytes[length++] = (byte) (number >>> shift);
                }
                continue;
            } else if (value instanceof byte[]) {
                valueBytes = (byte[]) value;
            } else if (value != null) {
                throw new IllegalArgumentException("CHAR requires numeric arguments: " + value);
            } else {
                continue;
            }
            int required = length + valueBytes.length;
            if (required > bytes.length) {
                byte[] grown = new byte[Math.max(required, bytes.length * 2)];
                System.arraycopy(bytes, 0, grown, 0, length);
                bytes = grown;
            }
            System.arraycopy(valueBytes, 0, bytes, length, valueBytes.length);
            length = required;
        }
        if (length == bytes.length) {
            return bytes;
        }
        byte[] exact = new byte[length];
        System.arraycopy(bytes, 0, exact, 0, length);
        return exact;
    }

    @Override
    public OpKey keyOf(@NonNull Type @NonNull ... types) {
        return OpKeys.ALL_STRING.keyOf(types);
    }

    @Override
    public @NonNull String getName() {
        return NAME;
    }

    public Type getType() {
        return Types.BYTES;
    }
}
