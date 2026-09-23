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

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;

/**
 * MySQL CONVERT(expr USING charset) for character sets. The binary charset is
 * handled separately so it retains its byte type instead of becoming text.
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
        Charset charset = CharCharsetFun.charset(value1.toString());
        try {
            if (value0 instanceof byte[]) {
                return charset.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap((byte[]) value0)).toString();
            }
            ByteBuffer bytes = charset.newEncoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT)
                .encode(CharBuffer.wrap(value0.toString()));
            return charset.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT)
                .decode(bytes).toString();
        } catch (CharacterCodingException e) {
            throw new IllegalArgumentException("Cannot convert value using charset " + charset.name(), e);
        }
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
