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

package io.dingodb.calcite.utils;

import io.dingodb.calcite.grammar.ddl.SqlAlterUser;
import io.dingodb.calcite.grammar.ddl.SqlCreateUser;
import io.dingodb.calcite.grammar.ddl.SqlSetPassword;
import org.apache.calcite.sql.DingoSqlBasicCall;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.util.Litmus;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

public class SqlUtil {
    public static String checkSql(SqlNode sqlNode, String sql) {
        if (sqlNode instanceof SqlCreateUser) {
            sql = ((SqlCreateUser) sqlNode).toLog();
        } else if (sqlNode instanceof SqlAlterUser) {
            sql = ((SqlAlterUser) sqlNode).toLog();
        } else if (sqlNode instanceof SqlSetPassword) {
            sql = ((SqlSetPassword) sqlNode).toLog();
        }
        if (!(sqlNode instanceof SqlSelect) && !(sqlNode instanceof SqlOrderBy) && sql.length() > 1000) {
            sql = sql.substring(0, 1000) + "...";
        }
        return sql;
    }

    public static boolean equalDeepIgnoreCast(List<? extends @Nullable SqlNode> operands0,
                                    List<? extends @Nullable SqlNode> operands1, Litmus litmus) {
        if (operands0.size() != operands1.size()) {
            return litmus.fail(null);
        }
        for (int i = 0; i < operands0.size(); i++) {
            if (!SqlNode.equalDeep(operands0.get(i), operands1.get(i), litmus)) {
                if (operands0.get(i) instanceof SqlBasicCall && operands1.get(i) instanceof SqlBasicCall) {
                    SqlBasicCall sqlBasicCall0 = (SqlBasicCall) operands0.get(i);
                    SqlBasicCall sqlBasicCall1 = (SqlBasicCall) operands1.get(i);
                    if (sqlBasicCall0.getKind() == SqlKind.CAST && sqlBasicCall1.getKind() != SqlKind.CAST) {
                        return sqlBasicCall0.operand(0).equalsDeep(sqlBasicCall1, litmus);
                    } else if (sqlBasicCall1.getKind() == SqlKind.CAST && sqlBasicCall0.getKind() != SqlKind.CAST) {
                        return sqlBasicCall1.operand(0).equalsDeep(sqlBasicCall0, litmus);
                    } else if (sqlBasicCall0.getKind() == SqlKind.CAST && sqlBasicCall1.getKind() == SqlKind.CAST) {
                        boolean checkOperand1 = sqlBasicCall0.operand(1).equalsDeep(sqlBasicCall1.operand(1), litmus);
                        if (!checkOperand1) {
                            return false;
                        }
                        SqlNode sqlNode0 = sqlBasicCall0.operand(0);
                        SqlNode sqlNode1 = sqlBasicCall1.operand(0);
                        if (sqlNode0 instanceof DingoSqlBasicCall && sqlNode1 instanceof SqlBasicCall) {
                            return sqlNode0.equalsDeep(sqlNode1, litmus);
                        } else if (sqlNode1 instanceof DingoSqlBasicCall && sqlNode0 instanceof SqlBasicCall) {
                            return sqlNode1.equalsDeep(sqlNode0, litmus);
                        }
                        return false;
                    }
                }
                return litmus.fail(null);
            }
        }
        return litmus.succeed();
    }
}
