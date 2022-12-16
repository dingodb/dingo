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

package io.dingodb.meta;

import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.annotation.ApiDeclaration;
import io.dingodb.common.auth.Authentication;
import io.dingodb.common.domain.Domain;
import io.dingodb.common.privilege.PrivilegeDefinition;
import io.dingodb.common.privilege.PrivilegeGather;
import io.dingodb.common.privilege.UserDefinition;
import io.dingodb.net.Channel;
import io.dingodb.net.Message;
import io.dingodb.net.NetService;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public interface SysInfoService {

    @ApiDeclaration
    boolean existsUser(UserDefinition userDefinition);

    @ApiDeclaration
    void createUser(UserDefinition userDefinition);

    @ApiDeclaration
    void dropUser(UserDefinition userDefinition);

    @ApiDeclaration
    void setPassword(UserDefinition userDefinition);

    @ApiDeclaration
    void grant(PrivilegeDefinition privilegeDefinition);

    @ApiDeclaration
    void revoke(PrivilegeDefinition privilegeDefinition);

    @ApiDeclaration
    PrivilegeGather getPrivilegeDef(Channel channel, String user, String host);

    @ApiDeclaration
    List<UserDefinition> getUserDefinition(String user);

    @ApiDeclaration
    public CommonId getSchemaId(String schema);

    default CommonId getSchemaIdByCache(String schema) {
        return Domain.INSTANCE.schemaIdMap.computeIfAbsent(schema, k -> {
            return getSchemaId(schema);
        });
    }

    @ApiDeclaration
    public CommonId getTableId(CommonId schemaId, String table);

    @ApiDeclaration
    public boolean verifyFollower(Authentication authentication);

    default CommonId getTableIdByCache(CommonId schemaId, String table) {
        Domain.INSTANCE.tableIdMap.computeIfAbsent(schemaId, k -> {
            Map<String, CommonId> tableIdMapping = new ConcurrentHashMap<>();
            tableIdMapping.put(table, getTableId(schemaId, table));
            return tableIdMapping;
        });
        return Domain.INSTANCE.tableIdMap.computeIfPresent(schemaId, (k, v) -> {
            v.computeIfAbsent(table, t ->
                 getTableId(schemaId, table)
            );
            return v;
        }).get(table);
    }

    default void flushPrivileges() {

    }
}
