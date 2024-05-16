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
import org.apache.calcite.sql.SqlNode;

public class SqlUtil {
    public static String checkSql(SqlNode sqlNode, String sql) {
        if (sqlNode instanceof SqlCreateUser) {
            sql = ((SqlCreateUser) sqlNode).toLog();
        } else if (sqlNode instanceof SqlAlterUser) {
            sql = ((SqlAlterUser) sqlNode).toLog();
        } else if (sqlNode instanceof SqlSetPassword) {
            sql = ((SqlSetPassword) sqlNode).toLog();
        }
        return sql;
    }
}
