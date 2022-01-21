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

import java.nio.charset.StandardCharsets;

/**
 * Common code, {@code [0, 1000)}.
 * Coordinator commands, {@code [1000, 1100)}.
 */
public interface Code {

    int BASE = 1000;
    int RAFT_SERVICE = 2000;
    int META_SERVICE = 3000;

    int code();

    /**
     * Generate the message of the null tag based on current code.
     * @return message
     */
    default Message message() {
        return message((Tag) null);
    }

    /**
     * Generate the message of the given tag based on current code.
     * @param tag tag
     * @return message
     */
    default Message message(Tag tag) {
        return message(tag, (byte[]) null);
    }

    /**
     * Generate the message of the given content based on current code.
     * @param content content
     * @return message
     */
    default Message message(String content) {
        return message(null, content);
    }

    /**
     * Generate the message of the given content based on current code.
     * @param content content
     * @return message
     */
    default Message message(byte[] content) {
        return message(null, content);
    }

    /**
     * Generate the message of the given tag and content based on current code.
     * @param tag tag
     * @return message
     */
    default Message message(Tag tag, String content) {
        return message(tag, content == null ? null : content.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Generate the message of the given tag and content based on current code.
     * @param tag tag
     * @return message
     */
    default Message message(Tag tag, byte[] content) {
        byte[] code = PrimitiveCodec.encodeZigZagInt(code());
        if (content == null) {
            return new SimpleMessage(tag, code);
        }
        byte[] realContent = new byte[code.length + content.length];
        System.arraycopy(code, 0, realContent, 0, code.length);
        System.arraycopy(content, 0, realContent, code.length, content.length);
        return new SimpleMessage(tag, realContent);
    }

    static Code valueOf(Integer code) {
        if (code < 0) {
            throw new IllegalArgumentException();
        }
        if (code < BASE) {
            return BaseCode.valueOf(code);
        }
        if (code < RAFT_SERVICE) {
            return RaftServiceCode.valueOf(code);
        }
        if (code < META_SERVICE) {
            return MetaServiceCode.valueOf(code);
        }
        throw new IllegalArgumentException(String.valueOf(code));
    }
}
