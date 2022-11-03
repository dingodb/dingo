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

package io.dingodb.expr.parser.value;

import io.dingodb.expr.core.TypeCode;
import io.dingodb.expr.parser.DefaultFunFactory;
import io.dingodb.expr.parser.Expr;
import io.dingodb.expr.runtime.CompileContext;
import io.dingodb.expr.runtime.RtConst;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.commons.text.StringEscapeUtils;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

@RequiredArgsConstructor
public class Value<T> implements Expr {
    @Getter
    private final T value;

    public static <T> @NonNull Value<T> of(T value) {
        return new Value<>(value);
    }

    /**
     * Create a Value from a String. {@code "false"} -&gt; {@code false} {@code "true"} -&gt; {@code true}
     *
     * @param text the String
     * @return the Value
     */
    public static @NonNull Value<Boolean> parseBoolean(String text) {
        return of(Boolean.parseBoolean(text));
    }

    public static @NonNull Value<Integer> parseInteger(String text) {
        return of(Integer.parseInt(text));
    }

    /**
     * Parse a String into a Value, by {@code Long::parseLong}.
     *
     * @param text the String
     * @return the Value
     */
    public static @NonNull Value<Long> parseLong(String text) {
        return of(Long.parseLong(text));
    }

    /**
     * Parse a String into a Value, by {@code Double::parseDouble}.
     *
     * @param text the String
     * @return the Value
     */
    @SuppressWarnings("unused")
    public static @NonNull Value<Double> parseDouble(String text) {
        return of(Double.parseDouble(text));
    }

    /**
     * Create a Value containing a String.
     *
     * @param text the String
     * @return the Value
     */
    public static @NonNull Value<String> parseString(@NonNull String text) {
        return of(StringEscapeUtils.unescapeJson(text.substring(1, text.length() - 1)));
    }

    public static @NonNull Value<BigDecimal> parseDecimal(String text) {
        return of(new BigDecimal(text));
    }

    public static @NonNull Expr parseInt(String text) {
        try {
            return Value.parseInteger(text);
        } catch (NumberFormatException e1) { // overflow
            try {
                return Value.parseLong(text);
            } catch (NumberFormatException e2) { // overflow
                return Value.parseDecimal(text);
            }
        }
    }

    public static @NonNull Expr parseReal(String text) {
        return Value.parseDecimal(text);
    }

    private static @NonNull String wrapByCast(int typeCode, String valueStr) {
        return DefaultFunFactory.castFunName(typeCode) + "(" + valueStr + ")";
    }

    @Override
    public @NonNull RtConst compileIn(CompileContext ctx) {
        return new RtConst(getValue());
    }

    @Override
    public String toString() {
        if (value instanceof String) {
            return "'" + StringEscapeUtils.escapeJson((String) value) + "'";
        }
        if (value instanceof Long
            && Integer.MIN_VALUE <= (Long) value
            && (Long) value <= Integer.MAX_VALUE
        ) {
            return wrapByCast(TypeCode.LONG, Long.toString((Long) value));
        }
        if (value instanceof BigDecimal
            && ((BigDecimal) value).scale() <= 0
            && 0 <= ((BigDecimal) value).compareTo(BigDecimal.valueOf(Long.MIN_VALUE))
            && ((BigDecimal) value).compareTo(BigDecimal.valueOf(Long.MAX_VALUE)) <= 0
        ) {
            return wrapByCast(TypeCode.DECIMAL, value.toString());
        }
        if (value instanceof Double) {
            return wrapByCast(TypeCode.DOUBLE, Double.toString((Double) value));
        }
        if (value instanceof Date) {
            return wrapByCast(TypeCode.DATE, Long.toString(((Date) value).getTime()));
        }
        if (value instanceof Time) {
            return wrapByCast(TypeCode.TIME, Long.toString(((Time) value).getTime()));
        }
        if (value instanceof Timestamp) {
            return wrapByCast(TypeCode.TIMESTAMP, Long.toString(((Timestamp) value).getTime()));
        }
        return value.toString();
    }
}
