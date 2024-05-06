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

package io.dingodb.calcite.rel;

import org.apache.calcite.linq4j.Nullness;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.ImmutableNullableList;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;

public class DingoBasicCall extends SqlBasicCall {
    private List<SqlNode> operandList;
    public DingoBasicCall(SqlBasicCall sqlBasicCall) {
        super(sqlBasicCall.getOperator(), sqlBasicCall.getOperandList(), sqlBasicCall.getParserPosition(), sqlBasicCall.getFunctionQuantifier());
        operandList = new ArrayList<>(sqlBasicCall.getOperandList());
    }

    @Override
    public List<SqlNode> getOperandList() {
        return this.operandList;
    }

    public SqlNode operand(int i) {
        return Nullness.castNonNull(this.operandList.get(i));
    }

    public int operandCount() {
        return this.operandList.size();
    }

    public void setOperand(int i, @Nullable SqlNode operand) {
        this.operandList = set(this.operandList, i, operand);
    }

    private static <E> List<E> set(List<SqlNode> list, int i, E e) {
        if (i == 0 && list.size() == 1) {
            return ImmutableNullableList.of(e);
        } else {
            E[] objects = (E[]) list.toArray();
            objects[i] = e;
            return ImmutableNullableList.copyOf(objects);
        }
    }

}
