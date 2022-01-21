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
 * Coordinator commands, {@code [1000, 2000)}.
 */
public enum RaftServiceCode implements Code {

    /**
     * Listen node state, when node become leader, send empty message to listener.
     */
    LISTEN_LEADER(1001),

    /**
     * Get leader node location.
     */
    GET_LEADER_LOCATION(1002),
    /**
     * Get all node location.
     */
    GET_ALL_LOCATION(1003),
    ;

    private int code;

    RaftServiceCode(int code) {
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

    private static Map<Integer, RaftServiceCode> valueOfCache;

    private static void addCache(int code, RaftServiceCode raftServiceCode) {
        if (valueOfCache == null) {
            valueOfCache = new HashMap<>();
        }
        valueOfCache.put(code, raftServiceCode);
    }

    public static RaftServiceCode valueOf(Integer code) {
        return valueOfCache.computeIfAbsent(
            code,
            k -> Arrays.stream(RaftServiceCode.values()).filter(c -> c.code == code).findAny().orElse(null)
        );
    }

}
