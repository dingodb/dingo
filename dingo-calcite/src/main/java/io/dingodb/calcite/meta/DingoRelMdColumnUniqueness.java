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

package io.dingodb.calcite.meta;

import io.dingodb.calcite.rel.DingoTableScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.ImmutableBitSet;
import org.checkerframework.checker.nullness.qual.NonNull;

public class DingoRelMdColumnUniqueness implements MetadataHandler<BuiltInMetadata.ColumnUniqueness> {
    public static final RelMetadataProvider SOURCE = ReflectiveRelMetadataProvider.reflectiveSource(
        new DingoRelMdColumnUniqueness(),
        BuiltInMetadata.ColumnUniqueness.Handler.class
    );

    private DingoRelMdColumnUniqueness() {
    }

    @Override
    public MetadataDef<BuiltInMetadata.ColumnUniqueness> getDef() {
        return BuiltInMetadata.ColumnUniqueness.DEF;
    }

    /**
     * This method is overridden to call {@link DingoTableScan#isKey(ImmutableBitSet)}. The default implementation
     * {@link org.apache.calcite.rel.metadata.RelMdColumnUniqueness#areColumnsUnique(TableScan, RelMetadataQuery,
     * ImmutableBitSet, boolean)} calls {@link org.apache.calcite.plan.RelOptTable#isKey(ImmutableBitSet)} directly,
     * which is not right if the table scan can do projection.
     * <p>
     * The column uniqueness is vital for calculating cost of `RelNodes` like `Aggregate`.
     */
    @SuppressWarnings({"unused", "MethodMayBeStatic"})
    public Boolean areColumnsUnique(
        @NonNull DingoTableScan rel,
        RelMetadataQuery mq,
        ImmutableBitSet columns,
        boolean ignoreNulls
    ) {
        return rel.isKey(columns);
    }
}
