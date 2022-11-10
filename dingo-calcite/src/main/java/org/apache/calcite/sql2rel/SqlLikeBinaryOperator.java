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

package org.apache.calcite.sql2rel;

import io.dingodb.calcite.grammar.SqlUserDefinedOperators;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.parser.SqlParserUtil;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlTypeUtil;

public class SqlLikeBinaryOperator extends SqlSpecialOperator {

    public static void register() {
        // Expand "x NOT LIKE BINARY y" into "NOT (x LIKE BINARY y)"
        StandardConvertletTable.INSTANCE.registerOp(SqlUserDefinedOperators.NOT_LIKE_BINARY,
            (cx, call) -> cx.convertExpression(
                SqlStdOperatorTable.NOT.createCall(SqlParserPos.ZERO,
                    SqlUserDefinedOperators.LIKE_BINARY.createCall(SqlParserPos.ZERO,
                        call.getOperandList()))));
    }

    private final boolean negated;
    private final boolean caseSensitive;

    /**
     * Creates a SqlLikeBinaryOperator.
     *
     * @param name        Operator name
     * @param kind        Kind
     * @param negated     Whether this is 'NOT LIKE BINARY'
     * @param caseSensitive Whether this operator ignores the case of its operands
     */
    public SqlLikeBinaryOperator(String name,
                                 SqlKind kind,
                                 boolean negated,
                                 boolean caseSensitive) {
        super(
            name,
            kind,
            32,
            false,
            ReturnTypes.BOOLEAN_NULLABLE,
            InferTypes.FIRST_KNOWN,
            OperandTypes.STRING_SAME_SAME);
        if (!caseSensitive && kind != SqlKind.OTHER_FUNCTION) {
            throw new IllegalArgumentException("Only (possibly negated) "
                + SqlKind.OTHER_FUNCTION + " can be made case-insensitive, not " + kind);
        }

        this.negated = negated;
        this.caseSensitive = caseSensitive;
    }

    /**
     * Returns whether this is the 'NOT LIKE BINARY' operator.
     *
     * @return whether this is 'NOT LIKE BINARY'
     *
     * @see #not()
     */
    public boolean isNegated() {
        return negated;
    }

    @Override public boolean checkOperandTypes(
        SqlCallBinding callBinding,
        boolean throwOnFailure) {
        switch (callBinding.getOperandCount()) {
            case 2:
                if (!OperandTypes.STRING_SAME_SAME.checkOperandTypes(
                    callBinding,
                    throwOnFailure)) {
                    return false;
                }
                break;
            case 3:
                if (!OperandTypes.STRING_SAME_SAME_SAME.checkOperandTypes(
                    callBinding,
                    throwOnFailure)) {
                    return false;
                }

                // calc implementation should
                // enforce the escape character length to be 1
                break;
            default:
                throw new AssertionError("unexpected number of args to "
                    + callBinding.getCall() + ": " + callBinding.getOperandCount());
        }

        return SqlTypeUtil.isCharTypeComparable(
            callBinding,
            callBinding.operands(),
            throwOnFailure);
    }

    @Override
    public ReduceResult reduceExpr(final int opOrdinal,
                                   TokenSequence list) {
        // Example:
        //   a LIKE b || c ESCAPE d || e AND f
        // |  |    |      |      |      |
        //  exp0    exp1          exp2
        SqlNode exp0 = list.node(opOrdinal - 1);
        SqlOperator op = list.op(opOrdinal);
         assert op instanceof SqlLikeBinaryOperator;
        SqlNode exp1 =
            SqlParserUtil.toTreeEx(
                list,
                opOrdinal + 1,
                getRightPrec(),
                SqlKind.ESCAPE);
        SqlNode exp2 = null;
        if ((opOrdinal + 2) < list.size()) {
            if (list.isOp(opOrdinal + 2)) {
                final SqlOperator op2 = list.op(opOrdinal + 2);
                if (op2.getKind() == SqlKind.ESCAPE) {
                    exp2 =
                        SqlParserUtil.toTreeEx(
                            list,
                            opOrdinal + 3,
                            getRightPrec(),
                            SqlKind.ESCAPE);
                }
            }
        }
        final SqlNode[] operands;
        final int end;
        if (exp2 != null) {
            operands = new SqlNode[]{exp0, exp1, exp2};
            end = opOrdinal + 4;
        } else {
            operands = new SqlNode[]{exp0, exp1};
            end = opOrdinal + 2;
        }
        SqlCall call = createCall(SqlParserPos.sum(operands), operands);
        return new ReduceResult(opOrdinal - 1, end, call);
    }

    @Override public void unparse(
        SqlWriter writer,
        SqlCall call,
        int leftPrec,
        int rightPrec) {
        final SqlWriter.Frame frame = writer.startList("", "");
        call.operand(0).unparse(writer, getLeftPrec(), getRightPrec());
        writer.sep(getName());

        call.operand(1).unparse(writer, getLeftPrec(), getRightPrec());
        if (call.operandCount() == 3) {
            writer.sep("ESCAPE");
            call.operand(2).unparse(writer, getLeftPrec(), getRightPrec());
        }
        writer.endList(frame);
    }

    @Override public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.between(2, 3);
    }

    @Override public SqlSyntax getSyntax() {
        return SqlSyntax.SPECIAL;
    }
}
