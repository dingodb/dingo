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
import io.dingodb.common.util.ByteArrayUtils;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.nio.ByteBuffer;

import static io.dingodb.common.util.ByteArrayUtils.EMPTY_BYTES;

@Getter
@EqualsAndHashCode
@AllArgsConstructor
public final class Message {

    public static final byte[] EMPTY_TAG = new byte[] {0};

    public static final Message EMPTY = new Message(EMPTY_TAG, EMPTY_BYTES);

    @Getter
    private final byte[] tag;
    @Getter
    private final byte[] content;
    private String tagStr;

    public Message(byte[] content) {
        this(EMPTY_TAG, content);
    }

    public Message(String tag) {
        this(tag, EMPTY_BYTES);
    }

    public Message(String tag, byte[] content) {
        this.tag = PrimitiveCodec.encodeString(tag);
        this.content = content;
        this.tagStr = tag;
    }

    public Message(byte[] tag, byte[] content) {
        this.tag = tag;
        this.content = content;
        this.tagStr = new String(tag);
    }

    public int length() {
        return tag.length + content.length;
    }

    public String tag() {
        return tagStr;
    }

    public byte[] content() {
        return content;
    }

    public byte[] encode() {
        return ByteArrayUtils.concatByteArray(tag, content);
    }

    public static Message decode(ByteBuffer buffer) {
        String tag = PrimitiveCodec.readString(buffer);
        byte[] content = new byte[buffer.remaining()];
        buffer.get(content);
        return new Message(tag, content);
    }
}
