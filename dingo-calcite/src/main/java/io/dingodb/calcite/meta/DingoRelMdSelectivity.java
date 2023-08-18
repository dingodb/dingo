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
import io.dingodb.calcite.stats.SelectivityEstimator;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdSelectivity;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.BuiltInMethod;

public class DingoRelMdSelectivity extends RelMdSelectivity {
    public static final RelMetadataProvider SOURCE =
        ReflectiveRelMetadataProvider.reflectiveSource(
            BuiltInMethod.SELECTIVITY.method, new DingoRelMdSelectivity());

    public Double getSelectivity(LogicalDingoTableScan tableScan, RelMetadataQuery mq, RexNode predicate) {
        double sel = 1.0;
        if ((predicate == null) || predicate.isAlwaysTrue()) {
            return sel;
        }
        SelectivityEstimator selectivityEstimator = new SelectivityEstimator(tableScan, mq);
        return selectivityEstimator.estimateSelectivity(predicate);
    }

}
