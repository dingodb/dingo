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

package io.dingodb.net.netty.utils;

import io.netty.buffer.ByteBuf;
import lombok.extern.slf4j.Slf4j;

import static io.dingodb.common.codec.PrimitiveCodec.INT_MAX_LEN;

@Slf4j
public class Serializers {

    private Serializers() {
    }

    /**
     * Read int from {@code bytes}, and use VarInt load.
     */
    public static Integer readVarInt(ByteBuf buf) {
        int readerIndex = buf.readerIndex();
        int maxBytes = INT_MAX_LEN;
        int b = Byte.MAX_VALUE + 1;
        int result = 0;
        while ((maxBytes >= 0) && b > Byte.MAX_VALUE) {
            if (!buf.isReadable()) {
                buf.readerIndex(readerIndex);
                return null;
            }
            result ^= ((b = (buf.readByte() & 0XFF)) & 0X7F) << ((INT_MAX_LEN - maxBytes--) * (Byte.SIZE - 1));
        }
        return result;
    }

}
