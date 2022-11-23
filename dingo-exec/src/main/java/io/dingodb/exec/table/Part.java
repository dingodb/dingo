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

package io.dingodb.exec.table;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public interface Part {
    @NonNull Iterator<Object[]> getIterator();

    @NonNull Iterator<Object[]> getIteratorByRange(
        byte[] startKey,
        byte[] endKey,
        boolean includeStart,
        boolean includeEnd,
        boolean prefixScan
    );

    long countDeleteByRange(byte[] startPrimaryKey, byte[] endPrimaryKey, boolean includeStart, boolean includeEnd);

    boolean insert(Object @NonNull [] tuple);

    void upsert(Object @NonNull [] tuple);

    boolean remove(Object @NonNull [] tuple);

    long getEntryCntAndDeleteByPart(@NonNull List<String> startKey);

    long getEntryCnt(@NonNull List<String> startKeyList);

    Object @Nullable [] getByKey(Object @NonNull [] keyTuple);

    default @NonNull List<Object[]> getByMultiKey(final @NonNull List<Object[]> keyTuples) {
        return keyTuples.stream()
            .map(this::getByKey)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
    }

    public Iterator<Object[]> keyValuePrefixScan(byte[] prefix);
}
