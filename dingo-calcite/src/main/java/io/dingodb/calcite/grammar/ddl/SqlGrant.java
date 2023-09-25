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

import io.dingodb.common.privilege.PrivilegeList;
import io.dingodb.common.privilege.PrivilegeType;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class SqlGrant extends SqlDdl {
    public List<String> privileges;
    public Boolean isAllPrivilege;

    public String schema;
    public String table;

    public SqlIdentifier tableIdentifier;

    public String user;
    public String host;

    public boolean withGrantOption;

    private static final SqlOperator OPERATOR =
        new SqlSpecialOperator("GRANT", SqlKind.OTHER_DDL);

    /**
     * Creates a SqlDdl.
     *
     * @param pos pos
     */
    public SqlGrant(SqlParserPos pos,
                    boolean isAllPrivilege,
                    List<String> privilege,
                    SqlIdentifier subject,
                    String user,
                    String host,
                    boolean withGrantOption) {
        super(OPERATOR, pos);
        this.isAllPrivilege = isAllPrivilege;
        this.privileges = privilege;
        this.schema = subject.names.get(0).toUpperCase();
        this.table = subject.names.get(1).toUpperCase();
        if (StringUtils.isBlank(table)) {
            table = "*";
        }
        if (!"*".equals(table)) {
            List<String> nameList = new ArrayList<>();
            nameList.add(schema);
            nameList.add(table);
            tableIdentifier = new SqlIdentifier(nameList, null, pos, new ArrayList<SqlParserPos>());
        }
        this.user = user.startsWith("'") ? user.replace("'", "") : user;
        this.host = host.startsWith("'") ? host.replace("'", "") : host;
        this.withGrantOption = withGrantOption;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return null;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("GRANT");
        AtomicInteger i = new AtomicInteger();
        privileges.forEach(k -> {
            if (!k.equalsIgnoreCase("grant")) {
                writer.keyword(k);
                i.getAndIncrement();
            }
        });
        if (i.get() == 0) {
            writer.keyword("USAGE");
        }
        writer.keyword("ON");
        writer.keyword(schema);
        writer.keyword(".");
        writer.keyword(table);
        writer.keyword("TO");
        writer.keyword(user);
        writer.keyword("@");
        writer.keyword(host);
        if (withGrantOption) {
            writer.keyword("with");
            writer.keyword("grant");
            writer.keyword("option");
        }
    }

    public List<String> getPrivileges(PrivilegeType privilegeType) {
        if (isAllPrivilege) {
            return PrivilegeList.privilegeMap.get(privilegeType);
        }
        validation(privilegeType);
        return privileges;
    }

    private void validation(PrivilegeType privilegeType) {
        privileges.forEach(privilege -> {
            if (!PrivilegeList.privilegeMap.get(privilegeType).contains(privilege)) {
                throw new RuntimeException("Illegal GRANT/REVOKE command");
            }
        });
    }
}
