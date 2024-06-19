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

package io.dingodb.calcite.rule;

import com.google.common.collect.ImmutableList;
import io.dingodb.calcite.traits.DingoRelCollationImpl;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalSort;

public final class RuleUtils {

    private RuleUtils() {
    }

    public static boolean validateDisableIndex(ImmutableList<RelHint> hints) {
        if (hints == null) {
            return false;
        }
        return !hints.isEmpty()
            && "disable_index".equalsIgnoreCase(hints.get(0).hintName);
    }

    public static boolean matchTablePrimary(LogicalSort logicalSort) {
        if (logicalSort.getCollation() instanceof DingoRelCollationImpl) {
            // order by primary cannot transform to indexScan
            DingoRelCollationImpl dingoRelCollation = (DingoRelCollationImpl) logicalSort.getCollation();
            return dingoRelCollation.isMatchPrimary();
        }
        return false;
    }

    public static int getSerialOrder(RelFieldCollation relFieldCollation) {
        int keepSerialOrder = 0;
        if (relFieldCollation.getDirection() == RelFieldCollation.Direction.ASCENDING) {
            keepSerialOrder = 1;
        } else if (relFieldCollation.getDirection() == RelFieldCollation.Direction.DESCENDING) {
            keepSerialOrder = 2;
        }
        return keepSerialOrder;
    }

    public static boolean preventRemoveOrder(int serialOrder) {
        return serialOrder == 2 || serialOrder == 0;
    }

    public static boolean matchRemoveSort(int primaryKeyIndex, String partitionStrategy) {
        return primaryKeyIndex == 0 && "range".equalsIgnoreCase(partitionStrategy);
    }
}
