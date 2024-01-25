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

import io.dingodb.calcite.rel.LogicalDingoTableScan;
import io.dingodb.calcite.stats.StatsCache;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.checkerframework.checker.nullness.qual.NonNull;

public class DingoRelMdRowCount implements MetadataHandler<BuiltInMetadata.RowCount> {
    public static final RelMetadataProvider SOURCE = ReflectiveRelMetadataProvider.reflectiveSource(
        new DingoRelMdRowCount(),
        BuiltInMetadata.RowCount.Handler.class
    );

    private DingoRelMdRowCount() {
    }

    @Override
    public MetadataDef<BuiltInMetadata.RowCount> getDef() {
        return BuiltInMetadata.RowCount.DEF;
    }

    /**
     * This method is overridden to call {@link SingleRel#estimateRowCount(RelMetadataQuery)}, the default
     * implementation {@link org.apache.calcite.rel.metadata.RelMdRowCount#getRowCount(SingleRel, RelMetadataQuery)}
     * does not.
     */
    @SuppressWarnings({"unused", "MethodMayBeStatic"})
    public Double getRowCount(@NonNull SingleRel rel, @NonNull RelMetadataQuery mq) {
        return rel.estimateRowCount(mq);
    }

    public Double getRowCount(@NonNull LogicalDingoTableScan rel, RelMetadataQuery mq) {
        return StatsCache.getTableRowCount(rel);
    }
}
