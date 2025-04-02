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

package org.apache.calcite.sql;

import com.google.common.collect.Iterables;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.calcite.util.Litmus;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;

import static io.dingodb.calcite.utils.SqlUtil.equalDeepIgnoreCast;

public class DingoSqlBasicCall extends SqlBasicCall {

    public DingoSqlBasicCall(SqlOperator operator, List<? extends @Nullable SqlNode> operandList, SqlParserPos pos) {
        super(operator, operandList, pos);
    }

    public DingoSqlBasicCall(SqlOperator operator, List<? extends @Nullable SqlNode> operandList, SqlParserPos pos, @Nullable SqlLiteral functionQualifier) {
        super(operator, operandList, pos, functionQualifier);
    }

    public static SqlCall createCall(
        SqlIdentifier funName,
        SqlParserPos pos,
        SqlFunctionCategory funcType,
        SqlLiteral functionQualifier,
        Iterable<? extends SqlNode> operands) {
        return createCall(funName, pos, funcType, functionQualifier,
            Iterables.toArray(operands, SqlNode.class));
    }

    protected static SqlCall createCall(
        SqlIdentifier funName,
        SqlParserPos pos,
        SqlFunctionCategory funcType,
        SqlLiteral functionQualifier,
        SqlNode[] operands) {
        // Create a placeholder function.  Later, during
        // validation, it will be resolved into a real function reference.
        SqlOperator fun = new SqlUnresolvedFunction(funName, null, null, null, null,
            funcType);

        pos = pos.plusAll(operands);
        return new DingoSqlBasicCall(fun, ImmutableNullableList.copyOf(operands), pos,
            functionQualifier);
    }



    @Override
    public boolean equalsDeep(@Nullable SqlNode node, Litmus litmus) {
        if (node == this) {
            return true;
        }
        if (!(node instanceof SqlCall)) {
            return litmus.fail("{} != {}", this, node);
        }
        SqlCall that = (SqlCall) node;

        // Compare operators by name, not identity, because they may not
        // have been resolved yet. Use case insensitive comparison since
        // this may be a case insensitive system.
        if (!this.getOperator().getName().equalsIgnoreCase(that.getOperator().getName())) {
            if (that.getOperator().getName().equalsIgnoreCase("EXTRACT")) {
                return that.getOperandList().get(0).toString().equalsIgnoreCase(this.getOperator().getName());
            } else if (this.getOperator().getName().equalsIgnoreCase("EXTRACT")) {
                return this.getOperandList().get(0).toString().equalsIgnoreCase(that.getOperator().getName());
            }
            return litmus.fail("{} != {}", this, node);
        }
        if (!equalDeep(this.getFunctionQuantifier(), that.getFunctionQuantifier(), litmus)) {
            return litmus.fail("{} != {} (function quantifier differs)", this, node);
        }
        boolean eqOperand = equalDeep(this.getOperandList(), that.getOperandList(), litmus);
        if (!eqOperand) {
            return equalDeepIgnoreCast(this.getOperandList(), that.getOperandList(), litmus);
        }
        return eqOperand;
    }

    @Override
    public SqlNode clone(SqlParserPos pos) {
        return getOperator().createCall(getFunctionQuantifier(), pos, this.getOperandList());
    }
}
