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

import java.nio.charset.StandardCharsets;

/** CONVERT(value USING BINARY) returns the UTF-8 bytes of textual values. */
public class ConvertBinaryFun extends BinaryOp {
    public static final String NAME = "convert_binary";
    private static final long serialVersionUID = 7820648382210847369L;
    public static final ConvertBinaryFun INSTANCE = new ConvertBinaryFun();

    @Override
    protected Object evalNonNullValue(@NonNull Object value, @NonNull Object charset, ExprConfig config) {
        return value instanceof byte[] ? value : value.toString().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public @NonNull String getName() {
        return NAME;
    }

    @Override
    public Type getType() {
        return Types.BYTES;
    }
}
