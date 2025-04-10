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

import com.google.common.collect.ImmutableList;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.util.Util;

import java.math.BigDecimal;
import java.util.Objects;

public class SqlConcatFunction extends SqlFunction {
    public static SqlConcatFunction CONCAT = new SqlConcatFunction();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates the SqlConcatFunction.
     */
    public SqlConcatFunction() {
        super(
            "CONCAT",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.ARG0_NULLABLE_VARYING,
            InferTypes.VARCHAR_1024,
            OperandTypes.or(OperandTypes.STRING_STRING_STRING, OperandTypes.STRING_STRING),
            SqlFunctionCategory.STRING);
    }

    //~ Methods ----------------------------------------------------------------

    @Override public String getSignatureTemplate(final int operandsCount) {
        switch (operandsCount) {
            case 2:
                return "{0}({1}, {2})";
            case 3:
                return "{0}({1}, {2}, {3})";
            default:
                throw new AssertionError();
        }
    }

    @Override public String getAllowedSignatures(String opName) {
        StringBuilder ret = new StringBuilder();
        for (Ord<SqlTypeName> typeName : Ord.zip(SqlTypeName.STRING_TYPES)) {
            if (typeName.i > 0) {
                ret.append(NL);
            }
            ret.append(
                SqlUtil.getAliasedSignature(this, opName,
                    ImmutableList.of(typeName.e, SqlTypeName.INTEGER)));
            ret.append(NL);
            ret.append(
                SqlUtil.getAliasedSignature(this, opName,
                    ImmutableList.of(typeName.e, SqlTypeName.INTEGER,
                        SqlTypeName.INTEGER)));
        }
        return ret.toString();
    }

    @Override public boolean checkOperandTypes(
        SqlCallBinding callBinding,
        boolean throwOnFailure) {
        if (this.getOperandTypeChecker() == null) {
            throw Util.needToImplement(this);
        } else {
            return this.getOperandTypeChecker().checkOperandTypes(callBinding, throwOnFailure);
        }
    }

    @Override public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.between(2, 3);
    }

    @Override public void unparse(
        SqlWriter writer,
        SqlCall call,
        int leftPrec,
        int rightPrec) {
        final SqlWriter.Frame frame = writer.startFunCall(getName());
        call.operand(0).unparse(writer, leftPrec, rightPrec);
        writer.sep(",");
        call.operand(1).unparse(writer, leftPrec, rightPrec);

        if (3 == call.operandCount()) {
            writer.sep(",");
            call.operand(2).unparse(writer, leftPrec, rightPrec);
        }

        writer.endFunCall(frame);
    }

    @Override public SqlMonotonicity getMonotonicity(SqlOperatorBinding call) {
        if (call.getOperandCount() == 3) {
            final SqlMonotonicity mono0 = call.getOperandMonotonicity(0);
            if (mono0 != null
                && mono0 != SqlMonotonicity.NOT_MONOTONIC
                && call.getOperandMonotonicity(1) == SqlMonotonicity.CONSTANT
                && Objects.equals(call.getOperandLiteralValue(1, Object.class), BigDecimal.ZERO)
                && call.getOperandMonotonicity(2) == SqlMonotonicity.CONSTANT) {
                return mono0.unstrict();
            }
        }
        return super.getMonotonicity(call);
    }

    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
        return opBinding.getTypeFactory().createSqlType(SqlTypeName.VARCHAR);
    }

}
