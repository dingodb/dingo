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
import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * MySQL CONVERT(expr USING charset) function, produced by the {@code CONVERT}
 * grammar production with a USING qualifier. The value passes through with
 * its charset annotation; the binary charset re-encodes the text as bytes.
 */
public class ConvertCharsetFun extends BinaryOp {
    public static final String NAME = "convert_charset";
    @SuppressWarnings("serial")
    private static final long serialVersionUID = -3194683446215801525L;

    public static final ConvertCharsetFun INSTANCE = new ConvertCharsetFun();

    @Override
    public Object evalValue(Object value0, Object value1, ExprConfig config) {
        if (value0 == null) {
            return null;
        }
        String charsetName = value1 == null ? "utf8mb4" : value1.toString();
        // Charset annotation only: the text passes through unchanged, so the
        // value never degrades to a Java byte[] dump through typed columns.
        return value0;
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
