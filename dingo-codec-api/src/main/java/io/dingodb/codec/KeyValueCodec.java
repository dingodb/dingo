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

package io.dingodb.codec;

import io.dingodb.common.store.KeyValue;
import org.checkerframework.checker.nullness.qual.NonNull;

public interface KeyValueCodec {

    /**
     * Encode the given tuple to key value.
     * @param tuple tuple
     * @return key value
     */
    KeyValue encode(Object @NonNull [] tuple);

    /**
     * Decode the given key value to tuple.
     * @param keyValue key value
     * @return tuple
     */
    Object[] decode(KeyValue keyValue);

    //

    /**
     * Encode the given tuple to byte array key.
     * @param tuple key tuple
     * @return key
     */
    byte[] encodeKey(Object[] tuple);

    /**
     * Decode the given key to tuple.
     * @param key key
     * @return key tuple
     */
    Object[] decodeKey(byte @NonNull [] key);

    //

    /**
     * Encode the given tuple to prefix format key.
     * @param tuple tuple
     * @param count key element count
     * @return prefix format key
     */
    byte[] encodeKeyPrefix(Object[] tuple, int count);

    /**
     * Decode the given prefix format key to tuple.
     * @param keyPrefix prefix format key
     * @return tuple
     */
    Object[] decodeKeyPrefix(byte[] keyPrefix);

    @Deprecated
    default Object[] mapKeyAndDecodeValue(Object[] keys, byte[] bytes) {
        throw new UnsupportedOperationException();
    }
}
