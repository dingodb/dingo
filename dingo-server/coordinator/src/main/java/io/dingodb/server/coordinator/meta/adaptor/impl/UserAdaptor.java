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
import io.dingodb.common.util.Optional;
import io.dingodb.server.coordinator.meta.adaptor.MetaAdaptorRegistry;
import io.dingodb.server.coordinator.meta.store.MetaStore;
import io.dingodb.server.protocol.meta.User;
import io.dingodb.verify.plugin.AlgorithmPlugin;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static io.dingodb.server.protocol.CommonIdConstant.ID_TYPE;
import static io.dingodb.server.protocol.CommonIdConstant.PRIVILEGE_IDENTIFIER;
import static io.dingodb.server.protocol.CommonIdConstant.ROOT_DOMAIN;

@Slf4j
public class UserAdaptor extends BaseAdaptor<User> {

    public static final CommonId META_ID = CommonId.prefix(ID_TYPE.data, PRIVILEGE_IDENTIFIER.user);

    protected final Map<String, User> userMap;

    public UserAdaptor(MetaStore metaStore) {
        super(metaStore);
        MetaAdaptorRegistry.register(User.class, this);
        userMap = new ConcurrentHashMap<>();
        metaMap.forEach((k, v) -> {
            userMap.put(v.getKey(), v);
        });
        log.info("init userMap:" + userMap);
        User user = User.builder().user("root")
            .host("%")
            .plugin("mysql_native_password")
            .password("cbcce4ebcf0e63f32a3d6904397792720f7e40ba")
            .build();
        user.setId(newId(user));
        userMap.putIfAbsent(user.getKey(), user);
    }

    @Override
    protected CommonId newId(User meta) {
        return new CommonId(
            META_ID.type(),
            META_ID.identifier(), ROOT_DOMAIN,
            metaStore.generateSeq(CommonId.prefix(META_ID.type(), META_ID.identifier()).encode())
        );
    }

    @Override
    public CommonId metaId() {
        return META_ID;
    }

    private UserDefinition metaToDefinition(User user) {
        if (user == null) {
            return null;
        }
        return UserDefinition.builder()
            .user(user.getUser())
            .host(user.getHost())
            .password(user.getPassword())
            .plugin(user.getPlugin())
            .build();
    }

    @AutoService(BaseAdaptor.Creator.class)
    public static class Creator implements BaseAdaptor.Creator<User, UserAdaptor> {
        @Override
        public UserAdaptor create(MetaStore metaStore) {
            return new UserAdaptor(metaStore);
        }
    }

    public CommonId create(UserDefinition userDefinition) {
        String userKey = userDefinition.getKey();
        if (userMap.containsKey(userKey)) {
            return userMap.get(userKey).getId();
        } else {
            User user = definitionToMeta(userDefinition);
            user.setId(newId(user));
            doSave(user);
            return user.getId();
        }
    }

    @Override
    protected void doSave(User user) {
        super.doSave(user);
        userMap.put(user.getKey(), user);
    }

    public boolean isExist(UserDefinition userDefinition) {
        log.info("user key:" + userDefinition.getKey() + ", userMap:" + userMap);
        if (userMap.containsKey(userDefinition.getKey())) {
            return true;
        }
        return false;
    }

    @Override
    protected void doDelete(User meta) {
        super.doDelete(meta);
    }

    public CommonId delete(UserDefinition userDefinition) {
        User user = userMap.remove(userDefinition.getKey());
        if (user != null) {
            this.doDelete(user);
            log.info("do delete userMap:" + userMap);
            return user.getId();
        } else {
            log.error("remove user is null");
        }
        return null;
    }

    public User getUser(String user, String host) {
        return Optional.ofNullable(userMap.get(user + "#%")).orElseGet(() -> userMap.get(user + "#" + host));
    }

    public UserDefinition getUserDefinition(String user, String host) {
        return metaToDefinition(getUser(user, host));
    }

    private User definitionToMeta(UserDefinition definition) {
        return User.builder().user(definition.getUser())
            .host(definition.getHost())
            .plugin(definition.getPlugin())
            .password(definition.getPassword())
            .build();
    }

    public void setPassword(UserDefinition definition) {
        User user = userMap.get(definition.getKey());
        String digestPwd = AlgorithmPlugin.digestAlgorithm(definition.getPassword(), user.getPlugin());
        user.setPassword(digestPwd);
        doSave(user);
        log.info("usermap:" + userMap);
    }
}
