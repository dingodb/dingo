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

package io.dingodb.calcite.type;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.sql.type.SqlTypeName;

public class DingoRelDataTypeSystemImpl extends RelDataTypeSystemImpl {

    public static RelDataTypeSystem DEFAULT = new DingoRelDataTypeSystemImpl();

    public static final int INTERVAL_START_PRECISION = 8;

    @Override
    public int getDefaultPrecision(SqlTypeName typeName) {
        // Following BasicSqlType precision as the default
        switch (typeName) {
            case CHAR:
            case BINARY:
            case BOOLEAN:
                return 1;
            case VARCHAR:
            case VARBINARY:
                return RelDataType.PRECISION_NOT_SPECIFIED;
            case DECIMAL:
                return 10;
            case INTERVAL_YEAR:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_MONTH:
            case INTERVAL_DAY:
            case INTERVAL_DAY_HOUR:
            case INTERVAL_DAY_MINUTE:
            case INTERVAL_DAY_SECOND:
            case INTERVAL_HOUR:
            case INTERVAL_HOUR_MINUTE:
            case INTERVAL_HOUR_SECOND:
            case INTERVAL_MINUTE:
            case INTERVAL_MINUTE_SECOND:
            case INTERVAL_SECOND:
                return INTERVAL_START_PRECISION;
            case TINYINT:
                return 3;
            case SMALLINT:
                return 5;
            case INTEGER:
                return 10;
            case BIGINT:
                return 19;
            case REAL:
                return 7;
            case FLOAT:
            case DOUBLE:
                return 15;
            case TIME:
            case TIME_WITH_LOCAL_TIME_ZONE:
            case DATE:
                return 0; // SQL99 part 2 section 6.1 syntax rule 30
            case TIMESTAMP:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                // farrago supports only 0 (see
                // SqlTypeName.getDefaultPrecision), but it should be 6
                // (microseconds) per SQL99 part 2 section 6.1 syntax rule 30.
                return 0;
            default:
                return -1;
        }
    }
}
