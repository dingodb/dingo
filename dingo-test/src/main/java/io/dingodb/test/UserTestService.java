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

package io.dingodb.test;

import io.dingodb.common.CommonId;
import io.dingodb.common.privilege.PrivilegeDefinition;
import io.dingodb.common.privilege.PrivilegeGather;
import io.dingodb.common.privilege.UserDefinition;
import io.dingodb.net.Channel;
import io.dingodb.verify.service.UserService;

public class UserTestService implements UserService {

    public static final UserTestService INSTANCE = new UserTestService();

    @Override
    public boolean existsUser(UserDefinition userDefinition) {
        return false;
    }

    @Override
    public void createUser(UserDefinition userDefinition) {

    }

    @Override
    public void dropUser(UserDefinition userDefinition) {

    }

    @Override
    public void setPassword(UserDefinition userDefinition) {

    }

    @Override
    public void grant(PrivilegeDefinition privilegeDefinition) {

    }

    @Override
    public void revoke(PrivilegeDefinition privilegeDefinition) {

    }

    @Override
    public PrivilegeGather getPrivilegeDef(Channel channel, String user, String host) {
        UserDefinition userDefinition = UserDefinition.builder().user("root").host("%")
            .password("cbcce4ebcf0e63f32a3d6904397792720f7e40ba").plugin("mysql_native_password").build();
        Boolean[] privileges = new Boolean[35];
        for (int i = 0; i < privileges.length; i ++) {
            privileges[i] = true;
        }
        return PrivilegeGather.builder().user("root").host("%").userDef(userDefinition).build();
    }

    @Override
    public UserDefinition getUserDefinition(String user, String host) {
        return UserDefinition.builder().user("root").host("%")
            .password("cbcce4ebcf0e63f32a3d6904397792720f7e40ba").plugin("mysql_native_password").build();
    }

    @Override
    public CommonId getSchemaId(String schema) {
        return null;
    }

    @Override
    public CommonId getTableId(CommonId schemaId, String table) {
        return null;
    }

}
