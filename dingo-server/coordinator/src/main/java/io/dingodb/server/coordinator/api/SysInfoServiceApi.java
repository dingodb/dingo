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

package io.dingodb.server.coordinator.api;

import io.dingodb.common.privilege.PrivilegeGather;
import io.dingodb.common.privilege.SchemaPrivDefinition;
import io.dingodb.common.privilege.TablePrivDefinition;
import io.dingodb.common.privilege.UserDefinition;
import io.dingodb.net.api.ApiRegistry;
import io.dingodb.server.coordinator.meta.adaptor.impl.SchemaPrivAdaptor;
import io.dingodb.server.coordinator.meta.adaptor.impl.TablePrivAdaptor;
import io.dingodb.server.coordinator.meta.adaptor.impl.UserAdaptor;
import io.dingodb.server.protocol.meta.SchemaPriv;
import io.dingodb.server.protocol.meta.TablePriv;
import io.dingodb.server.protocol.meta.User;

import java.util.List;

import static io.dingodb.server.coordinator.meta.adaptor.MetaAdaptorRegistry.getMetaAdaptor;

public class SysInfoServiceApi implements io.dingodb.server.api.SysInfoServiceApi {
    public SysInfoServiceApi() {
        ApiRegistry.getDefault().register(io.dingodb.server.api.SysInfoServiceApi.class, this);
    }

    @Override
    public void createUser(UserDefinition user) {

    }

    @Override
    public void dropUser(UserDefinition user) {

    }

    @Override
    public void grant(PrivilegeGather grants) {

    }

    @Override
    public void revoke(PrivilegeGather grants) {

    }

    @Override
    public PrivilegeGather getPrivilegeDef(String user) {
        List<SchemaPrivDefinition> schemaPrivileges = ((SchemaPrivAdaptor) getMetaAdaptor(SchemaPriv.class))
            .getSchemaPrivilege(user);
        List<UserDefinition> users = ((UserAdaptor) getMetaAdaptor(User.class)).getUserDefinition(user);
        List<TablePrivDefinition> tablePrivileges = ((TablePrivAdaptor) getMetaAdaptor(TablePriv.class))
            .getTablePrivilege(user);
        PrivilegeGather privilegeDefinition = PrivilegeGather.builder()
            .schemaPrivDefMap(schemaPrivileges)
            .userDefMap(users)
            .tablePrivDefMap(tablePrivileges)
            .build();
        return privilegeDefinition;
    }

    @Override
    public List<UserDefinition> getUserDefinition(String user) {
        return ((UserAdaptor) getMetaAdaptor(User.class)).getUserDefinition(user);
    }

}
