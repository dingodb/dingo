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
import io.dingodb.common.codec.ProtostuffCodec;
import io.dingodb.common.privilege.PrivilegeDefinition;
import io.dingodb.common.privilege.PrivilegeGather;
import io.dingodb.common.privilege.PrivilegeType;
import io.dingodb.common.privilege.SchemaPrivDefinition;
import io.dingodb.common.privilege.TablePrivDefinition;
import io.dingodb.common.privilege.UserDefinition;
import io.dingodb.common.util.Optional;
import io.dingodb.net.Channel;
import io.dingodb.net.Message;
import io.dingodb.net.NetService;
import io.dingodb.server.coordinator.meta.adaptor.MetaAdaptorRegistry;
import io.dingodb.server.coordinator.store.MetaStore;
import io.dingodb.server.protocol.Tags;
import io.dingodb.server.protocol.meta.Privilege;
import io.dingodb.server.protocol.meta.PrivilegeDict;
import io.dingodb.server.protocol.meta.Schema;
import io.dingodb.server.protocol.meta.SchemaPriv;
import io.dingodb.server.protocol.meta.Table;
import io.dingodb.server.protocol.meta.TablePriv;
import io.dingodb.server.protocol.meta.User;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import static io.dingodb.server.coordinator.meta.adaptor.MetaAdaptorRegistry.getMetaAdaptor;
import static io.dingodb.server.protocol.CommonIdConstant.ID_TYPE;
import static io.dingodb.server.protocol.CommonIdConstant.PRIVILEGE_IDENTIFIER;

@Slf4j
public class PrivilegeAdaptor extends BaseAdaptor<Privilege> {

    public static final CommonId META_ID = CommonId.prefix(ID_TYPE.privilege, PRIVILEGE_IDENTIFIER.privilege);

    protected final Map<CommonId, List<Privilege>> privilegeMap;

    public List<String> flushPrivileges = new CopyOnWriteArrayList<>();

    public List<Channel> channels = new CopyOnWriteArrayList<>();

    public PrivilegeAdaptor(MetaStore metaStore) {
        super(metaStore);
        MetaAdaptorRegistry.register(Privilege.class, this);
        privilegeMap = new ConcurrentHashMap<>();

        metaMap.forEach((k, v) -> privilegeMap.computeIfAbsent(v.getSubjectId(), p -> new ArrayList<>()).add(v));
        log.info("init privilegeMap:" + privilegeMap);
        NetService.getDefault().registerTagMessageListener(Tags.LISTEN_REGISTRY_RELOAD, this::registryReloadChannel);
        NetService.getDefault().registerTagMessageListener(Tags.LISTEN_RELOAD_PRIVILEGES, this::flushPrivileges);
    }

    @AutoService(BaseAdaptor.Creator.class)
    public static class Creator implements BaseAdaptor.Creator<Privilege, PrivilegeAdaptor> {
        @Override
        public PrivilegeAdaptor create(MetaStore metaStore) {
            return new PrivilegeAdaptor(metaStore);
        }
    }

    @Override
    protected CommonId newId(Privilege privilege) {
        return new CommonId(
            META_ID.type(),
            META_ID.identifier(), privilege.getSubjectId().seq(),
            metaStore.generateSeq(CommonId.prefix(META_ID.type(), META_ID.identifier()).encode())
        );
    }


    @Override
    public CommonId metaId() {
        return META_ID;
    }

    @Override
    protected void doSave(Privilege privilege) {
        super.doSave(privilege);
        privilegeMap.computeIfAbsent(privilege.getSubjectId(), k -> new ArrayList<>()).add(privilege);
    }

    private void registryReloadChannel(Message message, Channel channel) {
        if (!channels.contains(channel)) {
            channels.add(channel);
            List<String> privilege = getAllPrivilegeDict();
            channel.send(new Message(Tags.LISTEN_RELOAD_PRIVILEGE_DICT, ProtostuffCodec.write(privilege)));
        }
    }

    public List<String> getAllPrivilegeDict() {
        Map<String, CommonId> privilegeDict =
            ((PrivilegeDictAdaptor) getMetaAdaptor(PrivilegeDict.class)).getPrivilegeDict();
        return privilegeDict.entrySet().stream().map(this::mappingPrivilegeIndex).collect(Collectors.toList());
    }

    public String mappingPrivilegeIndex(Map.Entry<String, CommonId> entry) {
        return new StringBuilder(entry.getKey()).append("#").append(entry.getValue().seq()).toString();
    }

    private void flushPrivileges(Message message, Channel channel) {
        log.info("flush privileges, user:" + flushPrivileges.size() + ", channel size:" + channels.size());
        flushPrivileges.forEach(flush -> {
            String[] userIdentity = flush.split("#");
            String user = userIdentity[0];
            String host = userIdentity[1];
            channels.forEach(channel1 -> {
                PrivilegeGather privilegeGather = getPrivilegeGather(user, host);
                log.info("user:" + user + ",privilegeGather:" + privilegeGather
                    + ", channel:" + channel1.remoteLocation() + ", is active:" + channel1.isActive());
                if (channel1.isActive()) {
                    channel1.send(new Message(Tags.LISTEN_RELOAD_PRIVILEGES, ProtostuffCodec.write(privilegeGather)));
                }
            });
        });
        channel.close();
        log.info("flush privileges complete.");
    }

    @Override
    protected void doDelete(Privilege meta) {
        super.doDelete(meta);
    }

    public boolean delete(PrivilegeDefinition definition, CommonId subjectId) {
        List<Privilege> privileges = this.privilegeMap.computeIfPresent(subjectId, (k, v) -> {
            Iterator iterator = v.iterator();
            while (iterator.hasNext()) {
                Privilege privilege = (Privilege) iterator.next();
                if (definition.getPrivilegeIndexs().contains(privilege.getPrivilegeIndex())) {
                    iterator.remove();
                    this.doDelete(privilege);
                }
            }
            if (v.size() == 0) {
                return null;
            }
            return v;
        });
        if (!flushPrivileges.contains(definition.key())) {
            flushPrivileges.add(definition.key());
        }
        return privileges == null ? true : false;
    }

    public void create(PrivilegeDefinition definition, CommonId id) {
        delete(definition, id);
        definition.getPrivilegeIndexs().forEach(k -> {
            Privilege privilege = Privilege.builder()
                .host(definition.getHost())
                .user(definition.getUser())
                .privilegeIndex(k)
                .build();
            if (definition instanceof UserDefinition) {
                privilege.setPrivilegeType(PrivilegeType.USER);
            } else if (definition instanceof SchemaPrivDefinition) {
                privilege.setSchema(((SchemaPrivDefinition) definition).getSchema());
                privilege.setPrivilegeType(PrivilegeType.SCHEMA);
            } else if (definition instanceof TablePrivDefinition) {
                privilege.setSchema(((TablePrivDefinition) definition).getSchema());
                privilege.setTable(((TablePrivDefinition) definition).getTable());
                privilege.setPrivilegeType(PrivilegeType.TABLE);
            }
            privilege.setSubjectId(id);
            privilege.setId(newId(privilege));
            this.doSave(privilege);
        });
        if (!flushPrivileges.contains(definition.key())) {
            flushPrivileges.add(definition.key());
        }
        if (log.isDebugEnabled()) {
            log.debug("privilege map:" + privilegeMap);
        }
    }

    public void create(Privilege privilege) {
        privilege.setId(newId(privilege));
        this.doSave(privilege);
    }

    public List<UserDefinition> userDefinitions(List<User> users) {
        return users.stream().map(this :: metaToDefinition).collect(Collectors.toList());
    }

    public Map<CommonId, SchemaPrivDefinition> schemaPrivDefinitions(Map<CommonId, SchemaPriv> schemaPrivs) {
        return schemaPrivs.entrySet().stream().collect(Collectors.toMap(
            entry -> entry.getKey(), entry -> metaToDefinition(entry.getValue())));
    }

    public Map<CommonId, TablePrivDefinition> tablePrivDefinitions(Map<CommonId, TablePriv> tablePrivs) {
        Iterator<Map.Entry<CommonId, TablePriv>> it = tablePrivs.entrySet().iterator();
        TableAdaptor tableAdaptor = getMetaAdaptor(Table.class);
        while (it.hasNext()) {
            Map.Entry<CommonId, TablePriv> entry = it.next();
            if (tableAdaptor.get(entry.getValue().getTable()) == null) {
                it.remove();
            }

        }

        return tablePrivs.entrySet().stream().collect(Collectors.toMap(
            entry -> entry.getKey(), entry -> metaToDefinition(entry.getValue())));
    }

    public UserDefinition metaToDefinition(User user) {
        UserDefinition userDefinition = UserDefinition.builder().user(user.getUser())
            .host(user.getHost())
            .plugin(user.getPlugin())
            .password(user.getPassword())
            .build();
        Boolean[] privilegeIndexs = new Boolean[35];

        if (!"root".equalsIgnoreCase(user.getUser())) {
            for (int i = 0; i < privilegeIndexs.length; i ++) {
                privilegeIndexs[i] = false;
            }
            Optional.ofNullable(privilegeMap.get(user.getId())).ifPresent(privileges -> {
                privileges.forEach(privilege -> privilegeIndexs[privilege.getPrivilegeIndex()] = true);
            });
            userDefinition.setPrivileges(privilegeIndexs);
            return userDefinition;
        } else {
            for (int i = 0; i < privilegeIndexs.length; i ++) {
                privilegeIndexs[i] = true;
            }
            userDefinition.setPrivileges(privilegeIndexs);
            return userDefinition;
        }
    }

    public SchemaPrivDefinition metaToDefinition(SchemaPriv schemaPriv) {
        SchemaPrivDefinition schemaPrivDefinition = SchemaPrivDefinition.builder()
            .user(schemaPriv.getUser())
            .host(schemaPriv.getHost())
            .schema(schemaPriv.getSchema())
            //todo
            .schemaName("DINGO")
            .build();
        Boolean[] privilegeIndexs = new Boolean[35];
        for (int i = 0; i < privilegeIndexs.length; i ++) {
            privilegeIndexs[i] = false;
        }
        Optional.ofNullable(privilegeMap.get(schemaPriv.getId())).ifPresent(privileges -> {
            privileges.forEach(privilege -> privilegeIndexs[privilege.getPrivilegeIndex()] = true);
        });
        schemaPrivDefinition.setPrivileges(privilegeIndexs);
        return schemaPrivDefinition;
    }

    public TablePrivDefinition metaToDefinition(TablePriv tablePriv) {
        TableAdaptor tableAdaptor = getMetaAdaptor(Table.class);
        Table table = tableAdaptor.get(tablePriv.getTable());
        TablePrivDefinition tablePrivDefinition = TablePrivDefinition.builder()
            .user(tablePriv.getUser())
            .host(tablePriv.getHost())
            .schema(tablePriv.getSchema())
            //todo
            .schemaName("DINGO")
            .tableName(table.getName())
            .table(tablePriv.getTable())
            .build();
        Boolean[] privilegeIndexs = new Boolean[35];
        for (int i = 0; i < privilegeIndexs.length; i ++) {
            privilegeIndexs[i] = false;
        }
        Optional.ofNullable(privilegeMap.get(tablePriv.getId())).ifPresent(privileges -> {
            privileges.forEach(privilege -> privilegeIndexs[privilege.getPrivilegeIndex()] = true);
        });
        tablePrivDefinition.setPrivileges(privilegeIndexs);
        return tablePrivDefinition;
    }

    public PrivilegeGather getPrivilegeGather(String user, String host) {
        UserDefinition userDefinition = metaToDefinition(
            ((UserAdaptor) getMetaAdaptor(User.class)).getUser(user, host));

        Map<CommonId, SchemaPrivDefinition> schemaPrivDefinitions = schemaPrivDefinitions(
            ((SchemaPrivAdaptor) getMetaAdaptor(SchemaPriv.class)).getSchemaPrivilege(user, host));

        Map<CommonId, TablePrivDefinition> tablePrivDefinitions = tablePrivDefinitions(
            ((TablePrivAdaptor) getMetaAdaptor(TablePriv.class)).getTablePrivilege(user, host));

        return PrivilegeGather.builder()
            .userDef(userDefinition)
            .schemaPrivDefMap(schemaPrivDefinitions)
            .tablePrivDefMap(tablePrivDefinitions)
            .user(user)
            .host(userDefinition.getHost())
            .build();
    }

}
