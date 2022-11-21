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

package io.dingodb.server.coordinator.meta.adaptor.impl;

import com.google.auto.service.AutoService;
import io.dingodb.common.CommonId;
import io.dingodb.common.privilege.UserDefinition;
import io.dingodb.server.coordinator.meta.adaptor.MetaAdaptorRegistry;
import io.dingodb.server.coordinator.store.MetaStore;
import io.dingodb.server.protocol.meta.User;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.dingodb.server.protocol.CommonIdConstant.ID_TYPE;
import static io.dingodb.server.protocol.CommonIdConstant.TABLE_IDENTIFIER;

public class UserAdaptor extends BaseAdaptor<User> {

    //todo
    public static final CommonId META_ID = CommonId.prefix(ID_TYPE.other, TABLE_IDENTIFIER.table);

    public UserAdaptor(MetaStore metaStore) {
        super(metaStore);
        MetaAdaptorRegistry.register(User.class, this);
    }

    @Override
    protected CommonId newId(User meta) {
        return null;
    }

    @Override
    public CommonId metaId() {
        return META_ID;
    }

    public Map<String, List<UserDefinition>> getAllDefinition() {
        return getAll().stream().map(this::metaToDefinition)
            .collect(Collectors.groupingBy(UserDefinition::getUsername));
    }

    private UserDefinition metaToDefinition(User user) {
        return new UserDefinition();
    }

    @AutoService(BaseAdaptor.Creator.class)
    public static class Creator implements BaseAdaptor.Creator<User, UserAdaptor> {
        @Override
        public UserAdaptor create(MetaStore metaStore) {
            return new UserAdaptor(metaStore);
        }
    }

    public List<UserDefinition> getUserDefinition(String user) {
        // fake data
        List<UserDefinition> userDefinitions = new ArrayList<>();
        UserDefinition userDef = new UserDefinition();
        userDef.setUsername("root");
        userDef.setHost("172.27.128.1");
        userDef.setPassword("cbcce4ebcf0e63f32a3d6904397792720f7e40ba");
        userDef.setPlugin("mysql_native_password");

        UserDefinition userDef1 = new UserDefinition();
        userDef.setUsername("root");
        userDef.setHost("172.25.1.157");
        userDef.setPassword("cbcce4ebcf0e63f32a3d6904397792720f7e40ba");
        userDef.setPlugin("mysql_native_password");
        userDefinitions.add(userDef);
        userDefinitions.add(userDef1);
        return userDefinitions;
    }
}
