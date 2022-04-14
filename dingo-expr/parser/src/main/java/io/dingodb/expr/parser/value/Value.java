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

import io.dingodb.expr.parser.Expr;
import io.dingodb.expr.runtime.CompileContext;
import io.dingodb.expr.runtime.RtConst;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.commons.text.StringEscapeUtils;

import javax.annotation.Nonnull;

@RequiredArgsConstructor
public class Value<T> implements Expr {
    @Getter
    private final T value;

    @Nonnull
    public static <T> Value<T> of(T value) {
        return new Value<>(value);
    }

    /**
     * Create a Value from a String. {@code "false"} -&gt; {@code false} {@code "true"} -&gt; {@code true}
     *
     * @param text the String
     * @return the Value
     */
    @Nonnull
    public static Value<Boolean> parseBoolean(String text) {
        return of(Boolean.parseBoolean(text));
    }

    /**
     * Parse a String into a Value, by {@code Long::parseLong}.
     *
     * @param text the String
     * @return the Value
     */
    @Nonnull
    public static Value<Long> parseLong(String text) {
        return new Value<>(Long.parseLong(text));
    }

    /**
     * Parse a String into a Value, by {@code Double::parseDouble}.
     *
     * @param text the String
     * @return the Value
     */
    @Nonnull
    public static Value<Double> parseDouble(String text) {
        return new Value<>(Double.parseDouble(text));
    }

    /**
     * Create a Value containing a String.
     *
     * @param text the String
     * @return the Value
     */
    @Nonnull
    public static Value<String> parseString(@Nonnull String text) {
        return new Value<>(StringEscapeUtils.unescapeJson(text.substring(1, text.length() - 1)));
    }

    @Nonnull
    @Override
    public RtConst compileIn(CompileContext ctx) {
        return new RtConst(getValue());
    }

    @Override
    public String toString() {
        if (value instanceof String) {
            return "'" + StringEscapeUtils.escapeJson((String) value) + "'";
        }
        return value.toString();
    }
}
