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
import io.dingodb.server.coordinator.meta.adaptor.Adaptor;
import io.dingodb.server.protocol.meta.PrivilegeDict;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

import static io.dingodb.server.protocol.CommonIdConstant.ID_TYPE;
import static io.dingodb.server.protocol.CommonIdConstant.PRIVILEGE_IDENTIFIER;
import static io.dingodb.server.protocol.CommonIdConstant.ROOT_DOMAIN;

@Slf4j
@AutoService(Adaptor.class)
public class PrivilegeDictAdaptor extends BaseAdaptor<PrivilegeDict> {

    public static final CommonId META_ID = CommonId.prefix(ID_TYPE.data, PRIVILEGE_IDENTIFIER.privilegeDict);

    public static final CommonId privilegeDictId = new CommonId(
        ID_TYPE.data, PRIVILEGE_IDENTIFIER.privilegeType, ROOT_DOMAIN, 1, 1
    );

    public static final byte[] SEQ_KEY = META_ID.encode();

    private static final Map<String, CommonId> PRIVILEGE_DICT = new HashMap<>();

    static {
        PRIVILEGE_DICT.put("select", new CommonId(
            META_ID.type(), META_ID.identifier(), ROOT_DOMAIN, 1, 1
        ));
        PRIVILEGE_DICT.put("insert", new CommonId(
            META_ID.type(), META_ID.identifier(), ROOT_DOMAIN, 2, 1
        ));
        PRIVILEGE_DICT.put("update", new CommonId(
            META_ID.type(), META_ID.identifier(), ROOT_DOMAIN, 3, 1
        ));
        PRIVILEGE_DICT.put("delete", new CommonId(
            META_ID.type(), META_ID.identifier(), ROOT_DOMAIN, 4, 1
        ));
        PRIVILEGE_DICT.put("index", new CommonId(
            META_ID.type(), META_ID.identifier(), ROOT_DOMAIN, 5, 1
        ));
        PRIVILEGE_DICT.put("alter", new CommonId(
            META_ID.type(), META_ID.identifier(), ROOT_DOMAIN, 6, 1
        ));
        PRIVILEGE_DICT.put("create", new CommonId(
            META_ID.type(), META_ID.identifier(), ROOT_DOMAIN, 7, 1
        ));
        PRIVILEGE_DICT.put("drop", new CommonId(
            META_ID.type(), META_ID.identifier(), ROOT_DOMAIN, 8, 1
        ));
        PRIVILEGE_DICT.put("grant", new CommonId(
            META_ID.type(), META_ID.identifier(), ROOT_DOMAIN, 9, 1
        ));
        PRIVILEGE_DICT.put("create_view", new CommonId(
            META_ID.type(), META_ID.identifier(), ROOT_DOMAIN, 10, 1
        ));
        PRIVILEGE_DICT.put("show_view", new CommonId(
            META_ID.type(), META_ID.identifier(), ROOT_DOMAIN, 11, 1
        ));
        PRIVILEGE_DICT.put("create_routine", new CommonId(
            META_ID.type(), META_ID.identifier(), ROOT_DOMAIN, 12, 1
        ));
        PRIVILEGE_DICT.put("alter_routine", new CommonId(
            META_ID.type(), META_ID.identifier(), ROOT_DOMAIN, 13, 1
        ));
        PRIVILEGE_DICT.put("execute", new CommonId(
            META_ID.type(), META_ID.identifier(), ROOT_DOMAIN, 14, 1
        ));
        PRIVILEGE_DICT.put("trigger", new CommonId(
            META_ID.type(), META_ID.identifier(), ROOT_DOMAIN, 15, 1
        ));
        PRIVILEGE_DICT.put("event", new CommonId(
            META_ID.type(), META_ID.identifier(), ROOT_DOMAIN, 16, 1
        ));
        PRIVILEGE_DICT.put("create_tmp_table", new CommonId(
            META_ID.type(), META_ID.identifier(), ROOT_DOMAIN, 17, 1
        ));
        PRIVILEGE_DICT.put("lock_tables", new CommonId(
            META_ID.type(), META_ID.identifier(), ROOT_DOMAIN, 18, 1
        ));
        PRIVILEGE_DICT.put("references", new CommonId(
            META_ID.type(), META_ID.identifier(), ROOT_DOMAIN, 19, 1
        ));
        PRIVILEGE_DICT.put("reload", new CommonId(
            META_ID.type(), META_ID.identifier(), ROOT_DOMAIN, 20, 1
        ));
        PRIVILEGE_DICT.put("shutdown", new CommonId(
            META_ID.type(), META_ID.identifier(), ROOT_DOMAIN, 21, 1
        ));
        PRIVILEGE_DICT.put("process", new CommonId(
            META_ID.type(), META_ID.identifier(), ROOT_DOMAIN, 22, 1
        ));
        PRIVILEGE_DICT.put("file", new CommonId(
            META_ID.type(), META_ID.identifier(), ROOT_DOMAIN, 23, 1
        ));
        PRIVILEGE_DICT.put("show_db", new CommonId(
            META_ID.type(), META_ID.identifier(), ROOT_DOMAIN, 24, 1
        ));
        PRIVILEGE_DICT.put("super", new CommonId(
            META_ID.type(), META_ID.identifier(), ROOT_DOMAIN, 25, 1
        ));
        PRIVILEGE_DICT.put("repl_slave", new CommonId(
            META_ID.type(), META_ID.identifier(), ROOT_DOMAIN, 26, 1
        ));
        PRIVILEGE_DICT.put("repl_client", new CommonId(
            META_ID.type(), META_ID.identifier(), ROOT_DOMAIN, 27, 1
        ));
        PRIVILEGE_DICT.put("create_user", new CommonId(
            META_ID.type(), META_ID.identifier(), ROOT_DOMAIN, 28, 1
        ));
        PRIVILEGE_DICT.put("create_tablespace", new CommonId(
            META_ID.type(), META_ID.identifier(), ROOT_DOMAIN, 29, 1
        ));
        PRIVILEGE_DICT.put("extend1", new CommonId(
            META_ID.type(), META_ID.identifier(), ROOT_DOMAIN, 30, 1
        ));
        PRIVILEGE_DICT.put("extend2", new CommonId(
            META_ID.type(), META_ID.identifier(), ROOT_DOMAIN, 31, 1
        ));
        PRIVILEGE_DICT.put("extend3", new CommonId(
            META_ID.type(), META_ID.identifier(), ROOT_DOMAIN, 32, 1
        ));
        PRIVILEGE_DICT.put("extend4", new CommonId(
            META_ID.type(), META_ID.identifier(), ROOT_DOMAIN, 33, 1
        ));
        PRIVILEGE_DICT.put("extend5", new CommonId(
            META_ID.type(), META_ID.identifier(), ROOT_DOMAIN, 34, 1
        ));
    }

    @Override
    public synchronized void reload() {
        super.reload();
        if (this.metaMap.isEmpty()) {
            for (Map.Entry<String, CommonId> entry : PRIVILEGE_DICT.entrySet()) {
                PrivilegeDict privilegeDict = PrivilegeDict.builder()
                    .privilege(entry.getKey())
                    .id(entry.getValue())
                    .index(entry.getValue().seq())
                    .build();
                save(privilegeDict);
            }
        }
    }

    @Override
    public Class<PrivilegeDict> adaptFor() {
        return PrivilegeDict.class;
    }

    @Override
    protected CommonId newId(PrivilegeDict meta) {
        CommonId id = new CommonId(
            META_ID.type(), META_ID.identifier(), ROOT_DOMAIN, metaStore().generateSeq(SEQ_KEY), 1
        );
        return id;
    }

    @Override
    public CommonId metaId() {
        return META_ID;
    }

    public Map<String, CommonId> getPrivilegeDict() {
        return PRIVILEGE_DICT;
    }

}
