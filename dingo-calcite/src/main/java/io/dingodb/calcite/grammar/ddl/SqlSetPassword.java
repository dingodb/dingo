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

package io.dingodb.calcite.grammar.ddl;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSetOption;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Arrays;
import java.util.List;

public class SqlSetPassword extends SqlSetOption {
    public String user;
    public String host;
    public String password;
    private static final SqlIdentifier name = new SqlIdentifier(Arrays.asList("password"),
            null, new SqlParserPos(0, 0),
        Arrays.asList(new SqlParserPos(10, 1)));
    private static final SqlOperator OPERATOR =
        new SqlSpecialOperator("SET PASSWORD", SqlKind.OTHER_DDL);

    /**
     * Creates a SqlDdl.
     *
     * @param pos pos
     */
    public SqlSetPassword(SqlParserPos pos, String user, String host, String password) {
        super(pos, "user", name, name);
        this.user = user.startsWith("'") ? user.replace("'", "") : user;
        this.host = host.startsWith("'") ? host.replace("'", "") : host;
        this.password = password.startsWith("'") ? password.replace("'", "") : password;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return null;
    }

}
