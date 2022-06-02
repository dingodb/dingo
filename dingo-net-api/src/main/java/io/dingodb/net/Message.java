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

package io.dingodb.net;

import io.dingodb.common.codec.PrimitiveCodec;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.nio.ByteBuffer;

@Builder
@Getter
@EqualsAndHashCode
@AllArgsConstructor
public final class Message {

    public static final String EMPTY_TAG = "";

    public static final String API_OK = "API_OK";
    public static final String API_ERROR = "API_ERROR";

    public static final Message EMPTY = new Message(EMPTY_TAG, new byte[0]);

    private final String tag;
    private final byte[] content;

    public String tag() {
        return tag;
    }

    public byte[] content() {
        return content;
    }

    public byte[] encode() {
        byte[] tag = PrimitiveCodec.encodeString(this.tag);
        byte[] result = new byte[tag.length + content.length];
        System.arraycopy(tag, 0, result, 0, tag.length);
        System.arraycopy(content, 0, result, tag.length, content.length);
        return result;
    }

    public static Message decode(ByteBuffer buffer) {
        String tag = PrimitiveCodec.readString(buffer);
        byte[] content = new byte[buffer.remaining()];
        buffer.get(content);
        return new Message(tag, content);
    }

}
