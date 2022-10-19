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

package io.dingodb.common.partition;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeName;
import lombok.Getter;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

@JsonPropertyOrder({"type", "partNum"})
@JsonTypeName("simpleHash")
public final class SimplePartitionStrategy extends PartitionStrategy<String> {
    @JsonProperty("partNum")
    @Getter
    private final int partNum;

    @JsonCreator
    public SimplePartitionStrategy(
        @JsonProperty("partNum") int partNum
    ) {
        super();
        this.partNum = partNum;
    }

    @Override
    public @NonNull String calcPartId(Object @NonNull [] keyTuples) {
        int hash = Objects.hash(keyTuples);
        return Integer.toString(Math.abs(hash) % partNum);
    }

    @Override
    public @NonNull String calcPartId(byte @NonNull [] keyBytes) {
        int hash = Objects.hash(keyBytes);
        return Integer.toString(Math.abs(hash) % partNum);
    }

    @Override
    public @NonNull Map<byte[], byte[]> calcPartitionRange(
        byte @NonNull [] startKey,
        byte @NonNull [] endKey,
        boolean includeEnd
    ) {
        return Collections.emptyMap();
    }

    @Override
    public @Nullable Map<byte[], byte[]> calcPartitionPrefixRange(
        byte @NonNull [] startKey,
        byte @NonNull [] endKey,
        boolean includeEnd,
        boolean prefixRange
    ) {
        return null;
    }
}
