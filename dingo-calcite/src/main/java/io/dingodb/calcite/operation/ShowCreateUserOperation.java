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

package io.dingodb.calcite.operation;

import io.dingodb.common.privilege.UserDefinition;
import io.dingodb.verify.service.UserService;
import io.dingodb.verify.service.UserServiceProvider;
import lombok.Setter;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ShowCreateUserOperation implements QueryOperation {

    static UserService userService = UserServiceProvider.getRoot();

    @Setter
    public SqlNode sqlNode;

    private String userName;

    private String host;

    public ShowCreateUserOperation(SqlNode sqlNode, String userName, String host) {
        this.sqlNode = sqlNode;
        this.userName = userName;
        this.host = host;
    }

    @Override
    public Iterator getIterator() {
        List<Object[]> createUserList = new ArrayList<>();
        String createUser = getCreateUser();
        Object[] tuples = new Object[]{createUser};
        createUserList.add(tuples);
        return createUserList.iterator();
    }

    @Override
    public List<String> columns() {
        List<String> columns = new ArrayList<>();
        columns.add("Create User");
        return columns;
    }

    private String getCreateUser() {
        UserDefinition userDefinition = userService.getUserDefinition(this.userName, this.host);
        if (userDefinition == null) {
            throw new RuntimeException("Unknown user '" + this.userName + "'@'" + this.host + "'");
        }
        StringBuilder builder = new StringBuilder("CREATE USER '");
        builder.append(this.userName);
        builder.append("'@'");
        builder.append(this.host);
        builder.append("' IDENTIFIED WITH '");
        builder.append(userDefinition.getPlugin());
        builder.append("' AS '");
        builder.append(userDefinition.getPassword());
        builder.append("' REQUIRE ");
        String requireSsl = userDefinition.getRequireSsl();
        if (requireSsl == null || requireSsl.isEmpty()) {
            builder.append("NONE");
        } else {
            if ("ANY".equals(requireSsl)) {
                requireSsl = "SSL";
            }
            builder.append(requireSsl);
        }
        builder.append(" PASSWORD EXPIRE ");
        builder.append(userDefinition.getPasswordExpire().toString());
        builder.append(" ACCOUNT LOCK ");
        builder.append(userDefinition.getLock());

        return builder.toString();
    }
}
