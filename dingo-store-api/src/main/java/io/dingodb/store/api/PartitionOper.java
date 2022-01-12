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

package io.dingodb.store.api;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public interface PartitionOper {
    void create();

    Iterator<KeyValue> getIterator();

    boolean contains(byte[] key);

    void put(@Nonnull KeyValue keyValue);

    void delete(byte[] key);

    byte[] get(byte[] key);

    default List<byte[]> multiGet(@Nonnull List<byte[]> keys) {
        return keys.stream().map(this::get).collect(Collectors.toList());
    }
}
