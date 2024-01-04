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

package io.dingodb.client.vector;

import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.scalar.LongType;
import io.dingodb.sdk.common.codec.DingoKeyValueCodec;
import io.dingodb.sdk.common.codec.KeyValueCodec;
import io.dingodb.sdk.common.serial.schema.DingoSchema;
import io.dingodb.sdk.common.serial.schema.LongSchema;

import java.util.Arrays;
import java.util.Set;

import static java.util.Collections.singletonList;

public final class VectorKeyCodec {

    private VectorKeyCodec() {
    }

    public static final DingoSchema<Long> ID_SCHEMA;
    public static final DingoType ID_TYPE;
    public static final KeyValueCodec CODEC;
    public static final byte[] EMPTY;

    static {
        ID_SCHEMA =new LongSchema(0);
        ID_SCHEMA.setAllowNull(false);
        ID_SCHEMA.setIsKey(true);
        ID_TYPE = new LongType(false);
        CODEC = new DingoKeyValueCodec(0, singletonList(ID_SCHEMA));
        EMPTY = CODEC.encodeKeyPrefix(new Object[0], 0);
    }

    public static byte[] encode(long id) {
        return CODEC.encodeKeyPrefix(new Object[] {id}, 1);
    }

    public static long decode(byte[] key) {
        CODEC.resetPrefix(key, 0);
        Object[] keys = CODEC.decodeKeyPrefix(key);
        return keys[0] == null ? 0 : (long) keys[0];
    }

    public static void setEntityId(long entityId, byte[] key) {
        CODEC.resetPrefix(key, entityId);
    }

    public static byte[] nextEntityKey(long entityId) {
        byte[] key = Arrays.copyOf(EMPTY, EMPTY.length);
        setEntityId(entityId + 1, key);
        return key;
    }

}
