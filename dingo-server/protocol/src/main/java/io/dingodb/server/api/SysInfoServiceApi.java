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

package io.dingodb.server.api;

import io.dingodb.common.privilege.PrivilegeDefinition;
import io.dingodb.common.privilege.PrivilegeGather;
import io.dingodb.common.privilege.UserDefinition;
import io.dingodb.net.api.annotation.ApiDeclaration;

import java.util.List;

public interface SysInfoServiceApi {
    @ApiDeclaration
    void createUser(UserDefinition user);

    @ApiDeclaration
    void dropUser(UserDefinition user);

    @ApiDeclaration
    void grant(PrivilegeGather grants);

    @ApiDeclaration
    void revoke(PrivilegeGather grants);

    @ApiDeclaration
    PrivilegeGather getPrivilegeDef(String user);

    @ApiDeclaration
    List<UserDefinition> getUserDefinition(String user);
}
