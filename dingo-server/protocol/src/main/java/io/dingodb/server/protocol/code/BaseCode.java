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

package io.dingodb.server.protocol.code;

import io.dingodb.common.codec.PrimitiveCodec;
import io.dingodb.net.Message;
import io.dingodb.net.SimpleMessage;
import io.dingodb.net.Tag;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Common code, {@code [0, 1000)}.
 */
public enum BaseCode implements Code {

    /**
     * OK.
     */
    OK(0),

    /**
     * Ping.
     */
    PING(8),
    /**
     * Pong.
     */
    PONG(9),

    /**
     * Other.
     */
    OTHER(999),
    ;

    /**
     * Code.
     */
    private int code;

    BaseCode(int code) {
        this.code = code;
        addCache(code, this);
    }

    @Override
    public int code() {
        return code;
    }

    @Override
    public Message message(Tag tag, byte[] content) {

        return new SimpleMessage(tag, PrimitiveCodec.encodeZigZagInt(code));
    }

    private static Map<Integer, BaseCode> valueOfCache;

    private static void addCache(int code, BaseCode baseCode) {
        if (valueOfCache == null) {
            valueOfCache = new HashMap<>();
        }
        valueOfCache.put(code, baseCode);
    }

    public static BaseCode valueOf(Integer code) {
        return valueOfCache.computeIfAbsent(
            code,
            k -> Arrays.stream(BaseCode.values()).filter(c -> c.code == code).findAny().orElse(null)
        );
    }
}
