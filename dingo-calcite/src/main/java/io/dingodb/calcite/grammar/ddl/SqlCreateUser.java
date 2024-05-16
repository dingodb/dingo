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

import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

public class SqlCreateUser extends SqlCreate {
    public final String password;
    public final String user;
    public String host;
    public String requireSsl;
    public Object expireDays;
    public String lock;
    public String plugin;
    public String pluginDn;

    private static final SqlOperator OPERATOR =
        new SqlSpecialOperator("CREATE USER", SqlKind.OTHER_DDL);

    public SqlCreateUser(String user,
                         String password,
                         String host,
                         SqlParserPos pos,
                         boolean replace,
                         boolean ifNotExists,
                         String requireSsl,
                         String lock,
                         Object expireDays,
                         String plugin,
                         String pluginDn) {
        super(OPERATOR, pos, replace, ifNotExists);
        this.password = password.contains("'") ? password.replace("'", "") : password;
        this.user = user.contains("'") ? user.replace("'", "") : user;
        this.host = host == null ? "%" : host.contains("'") ? host.replace("'", "") : host;
        this.requireSsl = requireSsl;
        this.lock = lock;
        this.expireDays = expireDays;
        this.plugin = plugin == null ? "mysql_native_password" : plugin.contains("'") ? plugin.replace("'", ""): plugin;
        this.pluginDn = pluginDn == null ? "" : pluginDn.contains("'") ? pluginDn.replace("'", "") : pluginDn;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return null;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CREATE");
        writer.keyword("USER");
        writer.keyword(user);
        writer.keyword("@");
        writer.keyword(host);
        writer.keyword(" IDENTIFIED BY ");
        writer.keyword(password);
        if (StringUtils.isNotBlank(requireSsl)) {
            writer.keyword(" REQUIRE ");
            writer.keyword(requireSsl);
        }
        if (expireDays != null) {
            writer.keyword(" PASSWORD EXPIRE");
            if (!expireDays.equals("0")) {
                writer.keyword("INTERVAL");
                writer.keyword(expireDays.toString());
                writer.keyword("DAY");
            }
        }
        writer.keyword("ACCOUNT");
        if ("Y".equalsIgnoreCase(lock)) {
            writer.keyword("LOCK");
        } else if ("N".equalsIgnoreCase(lock)) {
            writer.keyword("UNLOCK");
        }
    }

    public String toLog() {
        StringBuilder str = new StringBuilder();
        str.append("CREATE").append(" USER ").append(user).append("@").append(host).append(" IDENTIFIED BY ").append("***");
        if (StringUtils.isNotBlank(requireSsl)) {
            str.append(" REQUIRE ");
            str.append(requireSsl);
        }
        if (expireDays != null) {
            str.append(" PASSWORD EXPIRE");
            if (!expireDays.equals("0")) {
                str.append(" INTERVAL ");
                str.append(expireDays.toString());
                str.append(" DAY");
            }
        }
        str.append(" ACCOUNT ");
        if ("Y".equalsIgnoreCase(lock)) {
            str.append("LOCK");
        } else if ("N".equalsIgnoreCase(lock)) {
            str.append("UNLOCK");
        }
        return str.toString();
    }
}
