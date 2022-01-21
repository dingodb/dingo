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
 * Coordinator commands, {@code [2000, 3000)}.
 */
public enum MetaServiceCode implements Code {

    /**
     * Listener table.
     */
    LISTENER_TABLE(2000),
    /**
     * Refresh tables.
     */
    REFRESH_TABLES(2001),
    /**
     * Get table.
     */
    GET_TABLE(2002),
    /**
     * Create table.
     */
    CREATE_TABLE(2003),
    /**
     * Delete table.
     */
    DELETE_TABLE(2004),
    /**
     * Get all.
     */
    GET_ALL(2005),
    ;

    private int code;

    MetaServiceCode(int code) {
        this.code = code;
        addCache(code, this);
    }

    @Override
    public int code() {
        return code;
    }

    @Override
    public Message message(Tag tag) {
        return new SimpleMessage(tag, PrimitiveCodec.encodeZigZagInt(code));
    }

    private static Map<Integer, MetaServiceCode> valueOfCache;

    private static void addCache(int code, MetaServiceCode metaServiceCode) {
        if (valueOfCache == null) {
            valueOfCache = new HashMap<>();
        }
        valueOfCache.put(code, metaServiceCode);
    }

    public static MetaServiceCode valueOf(Integer code) {
        return valueOfCache.computeIfAbsent(
            code,
            k -> Arrays.stream(MetaServiceCode.values()).filter(c -> c.code == code).findAny().orElse(null)
        );
    }

}
