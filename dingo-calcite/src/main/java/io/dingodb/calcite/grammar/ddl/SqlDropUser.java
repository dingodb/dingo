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

import org.apache.calcite.sql.SqlDrop;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

public class SqlDropUser extends SqlDrop {
    public final String name;

    public final String host;

    private static final SqlOperator OPERATOR =
        new SqlSpecialOperator("DROP USER", SqlKind.OTHER_DDL);

    public SqlDropUser(SqlParserPos pos, boolean ifExists, String name, String host) {
        super(OPERATOR, pos, ifExists);
        this.name = name.startsWith("'") ? name.replace("'", "") : name;
        if (StringUtils.isBlank(host)) {
            host = "%";
        }
        this.host = host.startsWith("'") ? host.replace("'", "") : host;;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return null;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("DROP");
        writer.keyword("USER");
        writer.keyword(name);
        writer.keyword(host);

    }
}
