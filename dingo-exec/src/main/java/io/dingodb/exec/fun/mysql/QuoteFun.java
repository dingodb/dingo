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
import io.dingodb.expr.runtime.op.UnaryOp;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;

/**
 * MySQL QUOTE() function.
 *
 * <p>Returns the argument as a quoted string suitable for use in an SQL
 * statement. The string is returned enclosed by single quotes and with each
 * instance of backslash (\), single quote ('), ASCII NUL, and Control-Z
 * preceded by a backslash. If the argument is NULL, the return value is the
 * word "NULL" without enclosing single quotation marks.</p>
 */
public class QuoteFun extends UnaryOp {
    public static final String NAME = "quote";
    @SuppressWarnings("serial")
    private static final long serialVersionUID = -8539149710792850411L;

    public static final QuoteFun INSTANCE = new QuoteFun();

    /**
     * Override the null gate so that SQL NULL input produces the four
     * character text 'NULL' instead of propagating SQL NULL.
     */
    @Override
    public Object evalValue(Object value, ExprConfig config) {
        return (value != null) ? evalNonNullValue(value, config) : "NULL";
    }

    @Override
    protected Object evalNonNullValue(@NonNull Object value, ExprConfig config) {
        String str;
        if (value instanceof byte[]) {
            try {
                str = StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap((byte[]) value)).toString();
            } catch (CharacterCodingException e) {
                throw new IllegalArgumentException("Cannot quote non-UTF-8 binary value", e);
            }
        } else {
            str = value.toString();
        }
        final int len = str.length();
        StringBuilder sb = new StringBuilder(len + 2);
        sb.append('\'');
        for (int i = 0; i < len; i++) {
            char c = str.charAt(i);
            switch (c) {
                case '\'':
                    sb.append('\\').append('\'');
                    break;
                case '\\':
                    sb.append('\\').append('\\');
                    break;
                case '\0':
                    sb.append('\\').append('0');
                    break;
                case '\032':
                    sb.append('\\').append('Z');
                    break;
                default:
                    sb.append(c);
                    break;
            }
        }
        sb.append('\'');
        return sb.toString();
    }

    @Override
    public @NonNull String getName() {
        return NAME;
    }

    @Override
    public Type getType() {
        return Types.STRING;
    }
}
