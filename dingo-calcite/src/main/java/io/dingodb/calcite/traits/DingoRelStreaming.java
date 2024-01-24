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

package io.dingodb.calcite.traits;

import com.google.common.collect.ImmutableSet;
import io.dingodb.common.CommonId;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTrait;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Set;

@EqualsAndHashCode(of = {"partitions", "distribution"})
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class DingoRelStreaming implements RelTrait {
    public static final DingoRelStreaming NONE = new DingoRelStreaming(null, null);
    public static final DingoRelStreaming ROOT = new DingoRelStreaming(ImmutableSet.of(), null);

    @Getter
    private final @Nullable Set<DingoRelPartition> partitions;
    @Getter
    private final @Nullable DingoRelPartition distribution;

    public static @NonNull DingoRelStreaming of(@NonNull RelOptTable table) {
        DingoRelPartition partition = DingoRelPartition.of(table);
        return new DingoRelStreaming(ImmutableSet.of(partition), partition);
    }

    public static @NonNull DingoRelStreaming of(@NonNull List<Integer> keys) {
        DingoRelPartition partition = DingoRelPartition.of(keys);
        return new DingoRelStreaming(ImmutableSet.of(partition), partition);
    }

    public static @NonNull DingoRelStreaming of(@NonNull CommonId indexId, @NonNull RelOptTable table) {
        DingoRelPartition partition = DingoRelPartition.of(indexId, table);
        return new DingoRelStreaming(ImmutableSet.of(partition), partition);
    }

    public boolean isRoot() {
        return partitions != null && partitions.isEmpty() && distribution == null;
    }

    public @NonNull DingoRelStreaming withPartition(@NonNull DingoRelPartition partition) {
        return withPartitions(ImmutableSet.of(partition));
    }

    public @NonNull DingoRelStreaming withPartitions(@NonNull Set<DingoRelPartition> partitions) {
        assert this.partitions != null;
        ImmutableSet.Builder<DingoRelPartition> builder = ImmutableSet.builder();
        builder.addAll(this.partitions);
        for (DingoRelPartition partition : partitions) {
            if (!this.partitions.contains(partition)) {
                builder.add(partition);
            }
        }
        return new DingoRelStreaming(builder.build(), distribution);
    }

    public @NonNull DingoRelStreaming removePartition(@NonNull DingoRelPartition partition) {
        assert this.partitions != null && partition != distribution;
        ImmutableSet.Builder<DingoRelPartition> builder = ImmutableSet.builder();
        for (DingoRelPartition p : this.partitions) {
            if (!partition.equals(p)) {
                builder.add(p);
            }
        }
        return new DingoRelStreaming(builder.build(), distribution);
    }

    public @NonNull DingoRelStreaming coalesced() {
        assert partitions != null && partitions.size() > 0;
        DingoRelPartition distribution = getDistribution();
        return distribution != null ? new DingoRelStreaming(ImmutableSet.of(distribution), distribution) : ROOT;
    }

    public @NonNull DingoRelStreaming changeDistribution(@Nullable DingoRelPartition distribution) {
        assert partitions != null;
        assert distribution == null || partitions.contains(distribution);
        return new DingoRelStreaming(partitions, distribution);
    }

    @Override
    public DingoRelStreamingDef getTraitDef() {
        return DingoRelStreamingDef.INSTANCE;
    }

    @Override
    public boolean satisfies(RelTrait trait) {
        return equals(trait);
    }

    @Override
    public void register(RelOptPlanner planner) {
    }

    @Override
    public String toString() {
        if (partitions != null) {
            return ((partitions.size() > 0) ? "PARTITIONED_BY" + partitions : "NO_PARTITION")
                + ", "
                + ((distribution != null) ? "DISTRIBUTED_BY[" + distribution + "]" : "NOT_DISTRIBUTED");
        }
        return "NO_STREAMING";
    }
}
