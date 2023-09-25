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

package io.dingodb.verify.service;

import io.dingodb.common.CommonId;
import io.dingodb.common.annotation.ApiDeclaration;
import io.dingodb.common.environment.ExecutionEnvironment;
import io.dingodb.common.privilege.PrivilegeDefinition;
import io.dingodb.common.privilege.PrivilegeGather;
import io.dingodb.common.privilege.UserDefinition;

public interface UserService {
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    @ApiDeclaration
    boolean existsUser(UserDefinition userDefinition);

    @ApiDeclaration
    void createUser(UserDefinition userDefinition);

    @ApiDeclaration
    void dropUser(UserDefinition userDefinition);

    @ApiDeclaration
    void updateUser(UserDefinition userDefinition);

    @ApiDeclaration
    void grant(PrivilegeDefinition privilegeDefinition);

    @ApiDeclaration
    void revoke(PrivilegeDefinition privilegeDefinition);

    @ApiDeclaration
    PrivilegeGather getPrivilegeDef(String user, String host);

    @ApiDeclaration
    UserDefinition getUserDefinition(String user, String host);

    CommonId getSchemaId(String schema);

    CommonId getTableId(String schemaName, String table);

    @ApiDeclaration
    void dropTablePrivilege(String schemaName, String tableName);

    default void flushPrivileges() {

    }
}
