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

import io.dingodb.calcite.rel.DingoGetByIndex;
import io.dingodb.calcite.rel.DingoGetByIndexMerge;
import io.dingodb.calcite.rel.DingoGetByKeys;
import io.dingodb.calcite.rel.DingoTableScan;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

public class DingoRelMdCost implements MetadataHandler<BuiltInMetadata.NonCumulativeCost> {

    private final DingoCostModel dingoCostModel;

    public static final RelMetadataProvider SOURCE = ReflectiveRelMetadataProvider.reflectiveSource(
        new DingoRelMdCost(),
        BuiltInMetadata.NonCumulativeCost.Handler.class
    );

    public DingoRelMdCost() {
        this.dingoCostModel = DingoCostModelV1.getCostModel();
    }

    @Override
    public MetadataDef<BuiltInMetadata.NonCumulativeCost> getDef() {
        return BuiltInMetadata.NonCumulativeCost.DEF;
    }

    public RelOptCost getNonCumulativeCost(DingoGetByIndex dingoGetByIndex, RelMetadataQuery mq) {
        return dingoCostModel.getDingoGetByIndex(dingoGetByIndex, mq);
    }

    public RelOptCost getNonCumulativeCost(DingoTableScan dingoTableScan, RelMetadataQuery mq) {
        return dingoCostModel.getDingoTableScan(dingoTableScan, mq);
    }

    public RelOptCost getNonCumulativeCost(DingoGetByKeys dingoGetByKeys, RelMetadataQuery mq) {
        return dingoCostModel.getDingoGetByKeys(dingoGetByKeys, mq);
    }

    public RelOptCost getNonCumulativeCost(DingoGetByIndexMerge dingoGetByIndexMerge, RelMetadataQuery mq) {
        return dingoCostModel.getDingoGetByIndexMerge(dingoGetByIndexMerge, mq);
    }

}
