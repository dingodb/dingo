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

package io.dingodb.common;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import io.dingodb.common.util.ByteArrayUtils;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.experimental.Accessors;

import java.io.IOException;
import java.io.Serializable;

import static io.dingodb.common.codec.PrimitiveCodec.decodeInt;
import static io.dingodb.common.codec.PrimitiveCodec.encodeInt;

@Accessors(fluent = true)
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class CommonId implements Comparable<CommonId>, Serializable {
    private static final long serialVersionUID = 3355195360067107406L;

    public static final int TYPE_LEN = 1;
    public static final int IDENTIFIER_LEN = 2;
    public static final int DOMAIN_LEN = 4;
    public static final int SEQ_LEN = 4;
    public static final int VER_LEN = 4;
    public static final int LEN = TYPE_LEN + IDENTIFIER_LEN + DOMAIN_LEN + SEQ_LEN + VER_LEN;

    public static final int TYPE_IDX = 0;
    public static final int IDENTIFIER_IDX = TYPE_IDX + TYPE_LEN;
    public static final int DOMAIN_IDX = IDENTIFIER_IDX + IDENTIFIER_LEN;
    public static final int SEQ_IDX = DOMAIN_IDX + DOMAIN_LEN;
    public static final int VER_IDX = SEQ_IDX + SEQ_LEN;

    @EqualsAndHashCode.Include
    private byte[] content;

    @Getter public final byte type;
    @Getter public final byte id0;
    @Getter public final byte id1;
    @Getter public final int domain;
    @Getter public final int seq;
    @Getter public final int ver;

    private transient volatile String str;

    public CommonId(byte type, byte[] identifier, int domain, int seq) {
        this(type, identifier, domain, seq, 1);
    }

    public CommonId(byte type, byte id0, byte id1, int domain, int seq) {
        this(type, id0, id1, domain, seq, 1);
    }

    public CommonId(byte type, byte[] identifier, int domain, int seq, int ver) {
        this(type, identifier[0], identifier[1], domain, seq, ver);
    }

    public CommonId(byte type, byte id0, byte id1, int domain, int seq, int ver) {
        this.type = type;
        this.id0 = id0;
        this.id1 = id1;
        this.domain = domain;
        this.seq = seq;
        this.ver = ver;
        initContent(type, id0, id1, domain, seq, ver);
    }

    private void initContent(byte type, byte id0, byte id1, int domain, int seq, int ver) {
        this.content = new byte[domain == 0 ? DOMAIN_IDX : seq == 0 ? SEQ_IDX : ver == 0 ? VER_IDX : LEN];
        content[0] = type;
        content[1] = id0;
        content[2] = id1;
        if (domain == 0) {
            return;
        }
        encodeInt(domain, content, DOMAIN_IDX);
        if (seq == 0) {
            return;
        }
        encodeInt(seq, content, SEQ_IDX);
        if (ver == 0) {
            return;
        }
        encodeInt(ver, content, VER_IDX);
    }

    @Override
    public int compareTo(CommonId other) {
        return ByteArrayUtils.compare(content, other.content);
    }

    public byte[] identifier() {
        return new byte[] {id0, id1};
    }

    public byte[] content() {
        byte[] content = new byte[this.content.length];
        System.arraycopy(this.content, 0, content, 0, content.length);
        return content;
    }

    @Override
    public String toString() {
        if (str == null) {
            this.str = new String(new byte[] {type, '-', id0, id1}) + '-' + domain + '-' + seq + '-' + ver;
        }
        return str;
    }

    public byte[] encode() {
        return encode(new byte[content.length], 0);
    }

    public byte[] encode(byte[] target, int index) {
        System.arraycopy(content, 0, target, index, target.length);
        return target;
    }

    public static CommonId decode(byte[] content) {
        return decode(content, 0);
    }

    public static CommonId decode(byte[] content, int index) {
        return new CommonId(
            content[index],
            content[index + 1], content[index + 2],
            content.length >= SEQ_IDX ? decodeInt(content, index + DOMAIN_IDX) : 0,
            content.length >= VER_IDX ? decodeInt(content, index + SEQ_IDX) : 0,
            content.length == LEN ? decodeInt(content, index + VER_IDX) : 0
        );
    }

    public static CommonId prefix(byte type, byte[] identifier) {
        return prefix(type, identifier, 0);
    }

    public static CommonId prefix(byte type, byte[] identifier, int domain) {
        return prefix(type, identifier, domain, 0);
    }

    public static CommonId prefix(byte type, byte[] identifier, int domain, int seq) {
        return new CommonId(type, identifier, domain, seq, 0);
    }

    public static class JacksonSerializer extends StdSerializer<CommonId> {
        protected JacksonSerializer() {
            super(CommonId.class);
        }

        @Override
        public void serialize(CommonId value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
            gen.writeBinary(value.content);
        }
    }

    public static class JacksonDeserializer extends StdDeserializer<CommonId> {
        protected JacksonDeserializer() {
            super(CommonId.class);
        }

        @Override
        public CommonId deserialize(JsonParser parser, DeserializationContext context) throws IOException {
            return decode(parser.getBinaryValue());
        }
    }
}
