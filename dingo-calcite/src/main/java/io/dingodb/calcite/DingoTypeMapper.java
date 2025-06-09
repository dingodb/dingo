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

package io.dingodb.calcite;

import io.dingodb.calcite.type.DingoSqlTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlSumAggFunction;
import org.apache.calcite.sql.type.SqlTypeName;

public class DingoTypeMapper {
    /*
        To be compatible with MYSQL aggregation.
        The relation between parameter type and result type are as following:
            SUM:
                -------------------------------------------------
                |    parameter type      |   result type        |
                -------------------------------------------------
                |    varchar             |   double             |
                |    char                |   double             |
                |    tinyint             |   decimal            |
                |    smallint            |   decimal            |
                |    mediumint           |   decimal            |
                |    int                 |   decimal            |
                |    bigint              |   decimal            |
                -------------------------------------------------
     */
    public static RelDataType getAggregateResultType(SqlAggFunction func, RelDataType sourceType) {
        //SUM
        if(func instanceof SqlSumAggFunction) {
            if (sourceType.getSqlTypeName() == SqlTypeName.VARCHAR) {
                return DingoSqlTypeFactory.INSTANCE.createSqlType(SqlTypeName.DOUBLE);
            } else if(sourceType.getSqlTypeName() == SqlTypeName.CHAR) {
                return DingoSqlTypeFactory.INSTANCE.createSqlType(SqlTypeName.DOUBLE);
            }
        }
        //TODO: Need adding more relations here to be compatible with MYSQL aggregation functions.

        //If no rule for mapper then we return the origin type.
        return null;
    }
}
