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

package io.dingodb.calcite.utils;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Pair;

import java.util.List;

public class DingoRelOptUtil {

    public static boolean equal(
        final String desc1,
        RelDataType type1,
        final String desc2,
        RelDataType type2,
        Litmus litmus) {
        if (!areRowTypesEqual(type1, type2, false)) {
            return litmus.fail(RelOptUtil.getFullTypeDifferenceString(desc1, type1, desc2, type2));
        }
        return litmus.succeed();
    }

    public static boolean areRowTypesEqual(
        RelDataType rowType1,
        RelDataType rowType2,
        boolean compareNames) {
        if (rowType1 == rowType2) {
            return true;
        }
        if (compareNames) {
            // if types are not identity-equal, then either the names or
            // the types must be different
            return false;
        }
        if (rowType2.getFieldCount() != rowType1.getFieldCount()) {
            return false;
        }
        final List<RelDataTypeField> f1 = rowType1.getFieldList();
        final List<RelDataTypeField> f2 = rowType2.getFieldList();
        for (Pair<RelDataTypeField, RelDataTypeField> pair : Pair.zip(f1, f2)) {
            final RelDataType type1 = pair.left.getType();
            final RelDataType type2 = pair.right.getType();
            // If one of the types is ANY comparison should succeed
            if (type1.getSqlTypeName() == SqlTypeName.ANY
                || type2.getSqlTypeName() == SqlTypeName.ANY) {
                continue;
            }
            if (!type1.equals(type2)) {
                if (type1.getSqlTypeName() == type2.getSqlTypeName()) {
                    continue;
                } else {
                    if ((type1.getSqlTypeName() != null && "NULL".equalsIgnoreCase(type1.getSqlTypeName().getName()))
                        || (type2.getSqlTypeName() != null && "NULL".equalsIgnoreCase(type2.getSqlTypeName().getName()))) {
                        continue;
                    }
                }
                return false;
            }
        }
        return true;
    }

}
