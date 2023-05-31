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
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.KeyDeserializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import io.dingodb.common.codec.PrimitiveCodec;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.ByteUtils;
import lombok.EqualsAndHashCode;
import lombok.NonNull;

import java.io.IOException;
import java.io.Serializable;

import static io.dingodb.common.codec.PrimitiveCodec.decodeLong;

@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class CommonId implements Comparable<CommonId>, Serializable {
    private static final long serialVersionUID = 3355195360067107406L;

    public static final int TYPE_LEN = 1;
    public static final int DOMAIN_LEN = 8;
    public static final int SEQ_LEN = 8;
    public static final int LEN = TYPE_LEN + DOMAIN_LEN + SEQ_LEN;

    public static final int TYPE_IDX = 0;
    public static final int DOMAIN_IDX = TYPE_IDX + TYPE_LEN;
    public static final int SEQ_IDX = DOMAIN_IDX + DOMAIN_LEN;

    public enum CommonType {
        TABLE(0),
        SCHEMA(1),
        DISTRIBUTION(2),
        OP(100);

        public final int code;

        CommonType(int code) {
            this.code = code;
        }

        public int getCode() {
            return code;
        }

        public static CommonType of(int code) {
            switch (code) {
                case 0: return TABLE;
                case 1: return SCHEMA;
                case 2: return DISTRIBUTION;
                case 100: return OP;
                default:
                    throw new IllegalStateException("Unexpected value: " + code);
            }
        }
    }

    public final CommonType type;
    public final long domain;
    public final long seq;

    private transient volatile byte[] content;
    @EqualsAndHashCode.Include
    private final transient String str;

    public CommonId(CommonType type, long domain, long seq) {
        this.type = type;
        this.domain = domain;
        this.seq = seq;
        this.str = type.name() + '_' + domain + '_' + seq;
    }

    @Override
    public int compareTo(@NonNull CommonId other) {
        return ByteArrayUtils.compare(encode(), other.encode());
    }

    @Override
    public String toString() {
        return str;
    }

    public byte[] encode() {
        if (content == null) {
            content = new byte[LEN];
            content[0] = (byte) type.code;
            PrimitiveCodec.encodeLong(domain, content, DOMAIN_IDX);
            PrimitiveCodec.encodeLong(seq, content, SEQ_IDX);
        }
        return content;
    }

    public byte[] encode(byte[] target, int index) {
        if (content == null) {
            encode();
        }
        System.arraycopy(content, 0, target, index, LEN);
        return target;
    }

    public static CommonId decode(byte[] content) {
        return decode(content, 0);
    }

    public static CommonId decode(byte[] content, int index) {
        return new CommonId(
            CommonType.of(content[index]),
            content.length >= SEQ_IDX ? decodeLong(content, index + DOMAIN_IDX) : 0,
            content.length >= LEN ? decodeLong(content, index + SEQ_IDX) : 0
        );
    }

    public static CommonId prefix(byte type, long domain) {
        return new CommonId(CommonType.of(type), domain, 0);
    }

    public static CommonId prefix(CommonType type, long domain) {
        return new CommonId(type, domain, 0);
    }


    public static class JacksonSerializer extends JsonSerializer<CommonId> {
        @Override
        public void serialize(CommonId value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
            gen.writeBinary(value.encode());
        }
    }

    public static class JacksonDeserializer extends JsonDeserializer<CommonId> {

        @Override
        public CommonId deserialize(JsonParser parser, DeserializationContext context) throws IOException {
            return decode(parser.getBinaryValue());
        }
    }

    public static class JacksonKeySerializer extends JsonSerializer<CommonId> {
        @Override
        public void serialize(CommonId value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
            gen.writeFieldName(ByteUtils.byteArrayToHexString(value.encode()));
        }
    }

    public static class JacksonKeyDeserializer extends KeyDeserializer {

        @Override
        public Object deserializeKey(String key, DeserializationContext ctxt) throws IOException {
            return decode(ByteUtils.hexStringToByteArray(key));
        }

    }

}
