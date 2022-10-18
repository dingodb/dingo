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

package io.dingodb.common.table;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface Part {
    @Nonnull
    Iterator<Object[]> getIterator();

    @Nonnull
    Iterator<Object[]> getIteratorByRange(byte[] startKey, byte[] endKey, boolean includeStart, boolean includeEnd,
                                          boolean prefixScan);

    long countDeleteByRange(byte[] startPrimaryKey, byte[] endPrimaryKey);

    boolean insert(@Nonnull Object[] tuple);

    void upsert(@Nonnull Object[] tuple);

    boolean remove(@Nonnull Object[] tuple);

    long getEntryCntAndDeleteByPart(@Nonnull List<String> startKey);

    long getEntryCnt(@Nonnull List<String> startKeyList);

    @Nullable
    Object[] getByKey(@Nonnull Object[] keyTuple);

    @Nonnull
    default List<Object[]> getByMultiKey(@Nonnull final List<Object[]> keyTuples) {
        return keyTuples.stream()
            .map(this::getByKey)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
    }
}
