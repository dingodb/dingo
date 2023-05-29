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

import org.apache.calcite.sql.SqlAlter;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

public class SqlAlterUser extends SqlAlter {

    public String password;
    public final String user;
    public String host;
    public String requireSsl;
    public String lock;
    public Object expireDays;

    private static final SqlOperator OPERATOR =
        new SqlSpecialOperator("ALTER USER", SqlKind.OTHER_DDL);

    public SqlAlterUser(String user, String password, String host,
                        String requireSsl,
                        SqlParserPos pos,
                        String lock,
                        Object expireDays) {
        super(pos);
        if (password != null) {
            this.password = password.contains("'") ? password.replace("'", "") : password;
        }
        this.user = user.contains("'") ? user.replace("'", "") : user;
        this.host = host == null ? "%" : host.contains("'") ? host.replace("'", "") : host;
        this.requireSsl = requireSsl;
        this.lock = lock;
        this.expireDays = expireDays;
    }

    @Override
    public void unparseAlterOperation(SqlWriter writer, int leftPrec, int rightPrec) {

    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return null;
    }
}
