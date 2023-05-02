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

import java.io.IOException;

public interface KeyValueCodec {
    Object[] decode(KeyValue keyValue) throws IOException;

    Object[] decodeKey(byte @NonNull [] bytes) throws IOException;

    KeyValue encode(Object @NonNull [] tuple) throws IOException;

    byte[] encodeKey(Object[] keys) throws IOException;

    byte[] encodeKeyPrefix(Object[] record, int columnCount) throws IOException;

    Object[] mapKeyAndDecodeValue(Object[] keys, byte[] bytes) throws IOException;
}
