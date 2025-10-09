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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.validate.SqlValidatorImpl;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

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

    public static final SqlReturnTypeInference CONTROL_FLOW_TYPE = new SqlReturnTypeInference() {
        // Some SqlOperator is invisible in this class, so we use String other than SqlOperator to recognize operators.
        private final Set<String> supportedOperators = ImmutableSet.of(
            "IF",
            "IFNULL",
            "COALESCE"
        );

        @Override
        public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
            Preconditions.checkArgument(opBinding instanceof SqlCallBinding,
                "this method must be invoke during validating, rather than RexNode building.");
            SqlCallBinding callBinding = (SqlCallBinding) opBinding;
            SqlCall ifCall = callBinding.getCall();
            String operatorName = callBinding.getOperator().getName().toUpperCase();

            if(!supportedOperators.contains(operatorName)) {
                throw new RuntimeException("Unsupported operator: " + operatorName);
            }
            int startPos = "IF".equals(operatorName) ? 1 : 0;

            List<SqlNode> operandList = ifCall.getOperandList();

            ArrayList<SqlNode> nullList = new ArrayList<>();
            List<RelDataType> argTypesNotNull = new ArrayList<>();

            for (int i = startPos; i < operandList.size(); i++) {
                SqlNode operand = operandList.get(i);
                if (SqlUtil.isNullLiteral(operand, false)) {
                    nullList.add(operand);
                } else {
                    RelDataType operandType = callBinding.getValidator().deriveType(callBinding.getScope(), operand);
                    argTypesNotNull.add(operandType);
                }
            }

            return returnTypeOfControlFlowFunction(nullList, argTypesNotNull, callBinding);
        }
    };

    private static RelDataType returnTypeOfControlFlowFunction(ArrayList<SqlNode> nullList, List<RelDataType> argTypesNotNull, SqlCallBinding callBinding) {
        RelDataType returnType;
        final RelDataTypeFactory typeFactory = callBinding.getTypeFactory();
        boolean isAllNulls = argTypesNotNull.isEmpty();
        boolean isAllNumeric = !isAllNulls && argTypesNotNull.stream().allMatch((t) -> SqlTypeUtil.isNumeric(t));
        boolean isAllDateTime = !isAllNulls && argTypesNotNull.stream().allMatch((t) -> SqlTypeUtil.isDatetime(t));
        boolean isAllString = !isAllNulls && argTypesNotNull.stream().allMatch((t) -> SqlTypeUtil.isString(t));
        boolean needStringType = !(isAllNumeric || isAllDateTime || isAllString);

        returnType = needStringType ?
            typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR, 2000), true) :
            typeFactory.leastRestrictive(argTypesNotNull);
        final SqlValidatorImpl validator = (SqlValidatorImpl) callBinding.getValidator();
        for (SqlNode node : nullList) {
            validator.setValidatedNodeType(node, returnType);
        }

        return returnType;
    }
}
