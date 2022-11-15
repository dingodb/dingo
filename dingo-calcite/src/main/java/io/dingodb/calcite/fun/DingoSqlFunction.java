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

package io.dingodb.calcite.fun;

import lombok.EqualsAndHashCode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

@EqualsAndHashCode(callSuper = true, onlyExplicitlyIncluded = true)
public class DingoSqlFunction extends SqlFunction {
    public DingoSqlFunction(
        String name,
        @Nullable SqlReturnTypeInference returnTypeInference,
        @Nullable SqlOperandTypeInference operandTypeInference,
        @Nullable SqlOperandTypeChecker operandTypeChecker,
        SqlFunctionCategory category
    ) {
        super(
            name,
            SqlKind.OTHER_FUNCTION,
            returnTypeInference,
            operandTypeInference,
            operandTypeChecker,
            category
        );
    }

    @Override
    public void validateCall(
        @NonNull SqlCall call,
        SqlValidator validator,
        SqlValidatorScope scope,
        SqlValidatorScope operandScope
    ) {
        SqlOperator operator = call.getOperator();
        assert getClass().isAssignableFrom(operator.getClass());
        super.validateCall(call, validator, scope, operandScope);
    }
}
