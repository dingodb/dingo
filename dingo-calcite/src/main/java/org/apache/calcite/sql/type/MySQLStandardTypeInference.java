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

package org.apache.calcite.sql.type;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlOperatorBinding;

public class MySQLStandardTypeInference {

    private static final String TIME_PART_FORMATS = "HISThiklrs";
    private static final String DATE_PART_FORMATS = "MVUXYWabcjmvuxyw";

    public static final SqlReturnTypeInference STR_TO_DATE = new SqlReturnTypeInference() {
        @Override
        public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
            final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();

            String format;
            if (!opBinding.isOperandLiteral(1, false)
                || (format = opBinding.getOperandLiteralValue(1, String.class)) == null
                || format.length() == 0) {
                return typeFactory.createSqlType(
                    SqlTypeName.TIMESTAMP,
                    typeFactory.getTypeSystem().getMaxPrecision(SqlTypeName.TIMESTAMP), 6);
            }

            byte[] formatAsBytes = format.getBytes();
            boolean fracSecondUsed = false, timePartUsed = false, datePartUsed = false;
            for (int i = 0; i < formatAsBytes.length; i++) {
                if (formatAsBytes[i] == '%' & i + 1 < formatAsBytes.length) {
                    i++;
                    if (formatAsBytes[i] == 'f') {
                        fracSecondUsed = true;
                        timePartUsed = true;
                    } else if (!timePartUsed && TIME_PART_FORMATS.indexOf(formatAsBytes[i]) != -1) {
                        timePartUsed = true;
                    } else if (!datePartUsed && DATE_PART_FORMATS.indexOf(formatAsBytes[i]) != -1) {
                        datePartUsed = true;
                    }

                    if (datePartUsed && fracSecondUsed) {
                        // frac_second_used implies time_part_used, and thus we already
                        // have all types of date-time components and can end our search.
                        return typeFactory.createSqlType(
                            SqlTypeName.TIMESTAMP,
                            typeFactory.getTypeSystem().getMaxPrecision(SqlTypeName.TIMESTAMP), 6);
                    }
                }
            }

            //  We don't have all three types of date-time components
            if (fracSecondUsed) {
                // TIME with microseconds
                return typeFactory.createSqlType(
                    SqlTypeName.TIME,
                    typeFactory.getTypeSystem().getMaxPrecision(SqlTypeName.TIME), 6);
            } else if (timePartUsed) {
                if (datePartUsed) {
                    // DATETIME, no microseconds
                    return typeFactory.createSqlType(
                        SqlTypeName.TIMESTAMP,
                        typeFactory.getTypeSystem().getMaxPrecision(SqlTypeName.TIMESTAMP), 0);
                } else {
                    // TIME, no microseconds
                    return typeFactory.createSqlType(
                        SqlTypeName.TIME,
                        typeFactory.getTypeSystem().getMaxPrecision(SqlTypeName.TIME), 0);
                }
            } else {
                // DATE
                return typeFactory.createSqlType(SqlTypeName.DATE);
            }
        }
    };
}
