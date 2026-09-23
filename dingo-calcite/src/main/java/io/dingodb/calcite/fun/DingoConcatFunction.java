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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.nio.charset.Charset;

@EqualsAndHashCode(callSuper = true, onlyExplicitlyIncluded = true)
public class DingoConcatFunction extends DingoSqlFunction {
    public DingoConcatFunction(
        String name,
        @Nullable SqlReturnTypeInference returnTypeInference,
        @Nullable SqlOperandTypeInference operandTypeInference,
        @Nullable SqlOperandTypeChecker operandTypeChecker,
        SqlFunctionCategory category
    ) {
        super(
            name,
            returnTypeInference == null ? null : binding -> inferCharsetReturnType(binding, returnTypeInference),
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

    private static RelDataType inferCharsetReturnType(
        SqlOperatorBinding binding, SqlReturnTypeInference returnTypeInference
    ) {
        RelDataType result = returnTypeInference.inferReturnType(binding);
        RelDataType chosen = null;
        int chosenIndex = -1;
        for (int i = 0; i < binding.getOperandCount(); i++) {
            RelDataType operand = binding.getOperandType(i);
            if (operand.getCharset() == null || operand.getCollation() == null) {
                continue;
            }
            if (chosen != null
                && !chosen.getCharset().equals(operand.getCharset())
                && chosen.getCollation().getCoercibility() == operand.getCollation().getCoercibility()) {
                Charset left = chosen.getCharset();
                Charset right = operand.getCharset();
                boolean leftLiteral = binding.isOperandLiteral(chosenIndex, false);
                boolean rightLiteral = binding.isOperandLiteral(i, false);
                if (leftLiteral != rightLiteral) {
                    String literal = binding.getOperandLiteralValue(
                        leftLiteral ? chosenIndex : i, String.class
                    );
                    Charset target = leftLiteral ? right : left;
                    if (literal != null && target.newEncoder().canEncode(literal)) {
                        // Literals are weaker than character expressions when conversion is lossless.
                        if (leftLiteral) {
                            chosen = operand;
                            chosenIndex = i;
                        }
                        continue;
                    }
                }
                if (isUnicode(left) != isUnicode(right)) {
                    // At equal coercibility MySQL promotes non-Unicode text to Unicode.
                    if (isUnicode(right)) {
                        chosen = operand;
                        chosenIndex = i;
                    }
                } else if (!left.contains(right)) {
                    if (right.contains(left)) {
                        chosen = operand;
                        chosenIndex = i;
                    } else {
                        throw new IllegalArgumentException("Incompatible CONCAT character sets: "
                            + left.name() + " and " + right.name());
                    }
                }
                continue;
            }
            if (chosen == null || SqlCollation.getCoercibilityDyadicOperator(
                chosen.getCollation(), operand.getCollation()
            ) == operand.getCollation()) {
                chosen = operand;
                chosenIndex = i;
            }
        }
        return chosen == null ? result : binding.getTypeFactory().createTypeWithCharsetAndCollation(
            result, chosen.getCharset(), chosen.getCollation()
        );
    }

    private static boolean isUnicode(Charset charset) {
        return charset.name().startsWith("UTF-");
    }

}
