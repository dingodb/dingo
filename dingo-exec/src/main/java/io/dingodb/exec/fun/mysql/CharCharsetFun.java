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

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * MySQL CHAR(... USING charset) function, produced by the {@code CHAR}
 * grammar production with a USING qualifier. The charset appends to the
 * argument list as a trailing string literal.
 */
public class CharCharsetFun extends VariadicOp {
    public static final String NAME = "char_charset";
    @SuppressWarnings("serial")
    private static final long serialVersionUID = -4653090521145395023L;

    public static final CharCharsetFun INSTANCE = new CharCharsetFun();

    @Override
    public Object evalValue(Object @NonNull [] values, ExprConfig config) {
        if (values.length == 0) {
            return "";
        }
        String charsetName = values[values.length - 1].toString();
        byte[] bytes = CharFun.buildBytes(values, values.length - 1);
        return new String(bytes, charset(charsetName));
    }

    /**
     * Map MySQL charset names onto Java charsets. Unknown names fall back to
     * UTF-8; {@code binary} uses ISO-8859-1 so every byte round-trips.
     */
    static Charset charset(@NonNull String name) {
        String normalized = name.trim().toLowerCase();
        switch (normalized) {
            case "utf8":
            case "utf-8":
            case "utf8mb4":
                return StandardCharsets.UTF_8;
            case "binary":
            case "latin1":
                return StandardCharsets.ISO_8859_1;
            case "gbk":
                return Charset.forName("GBK");
            default:
                return StandardCharsets.UTF_8;
        }
    }

    @Override
    public OpKey keyOf(@NonNull Type @NonNull ... types) {
        return OpKeys.ALL_STRING.keyOf(types);
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
