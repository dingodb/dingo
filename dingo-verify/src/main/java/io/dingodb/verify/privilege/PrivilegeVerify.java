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

package io.dingodb.verify.privilege;

import io.dingodb.common.CommonId;
import io.dingodb.common.auth.Authentication;
import io.dingodb.common.auth.DingoRole;
import io.dingodb.common.config.SecurityConfiguration;
import io.dingodb.common.environment.ExecutionEnvironment;
import io.dingodb.common.privilege.DingoSqlAccessEnum;
import io.dingodb.common.privilege.PrivilegeDefinition;
import io.dingodb.common.privilege.PrivilegeDict;
import io.dingodb.common.privilege.PrivilegeGather;
import io.dingodb.common.privilege.SchemaPrivDefinition;
import io.dingodb.common.privilege.TablePrivDefinition;
import io.dingodb.common.privilege.UserDefinition;
import io.dingodb.net.Channel;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.Collection;
import java.util.List;

@Slf4j
public class PrivilegeVerify {

    static ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    static {
        SecurityConfiguration configuration = SecurityConfiguration.instance();
        isVerify = configuration.isVerify();
    }

    public static boolean isVerify;

    public static boolean verify(Channel channel, CommonId schema, CommonId table, DingoSqlAccessEnum accessType) {
        if (!isVerify) {
            return true;
        }

        Object[] objects = channel.auth().get("token");
        if (objects == null) {
            throw new IllegalArgumentException("Access denied, invalid parameter.");
        }
        Authentication authentication = (Authentication) objects[0];
        String user = authentication.getUsername();
        String host = authentication.getHost();
        return verify(user, host, schema, table, accessType);
    }

    public static boolean verify(String user, String host, CommonId schema, CommonId table,
                             DingoSqlAccessEnum accessType) {
        if (!isVerify) {
            return true;
        }
        if (DingoRole.SQLLINE == env.getRole()) {
            return true;
        }
        if (StringUtils.isBlank(user)) {
            return true;
        }
        PrivilegeGather privilegeGather = env.getPrivilegeGatherMap().get(user + "#" + host);
        if (!verify(user, host, schema, table, accessType, privilegeGather)) {
            privilegeGather = env.getPrivilegeGatherMap().get(user + "#%");
            return verify(user, host, schema, table, accessType, privilegeGather);
        }
        return true;
    }

    public static boolean verify(String user, String host, CommonId schema, CommonId table,
                                 DingoSqlAccessEnum accessType, PrivilegeGather privilegeGather) {
        if (privilegeGather == null) {
            return false;
        }
        if (log.isDebugEnabled()) {
            log.debug(" privilege for {} @ {} detail:" + privilegeGather, user, host);
        }
        Integer index = PrivilegeDict.privilegeIndexDict.get(accessType.getAccessType());
        if (index == null) {
            return true;
        }

        UserDefinition userDef = privilegeGather.getUserDef();
        if (userDef != null && userDef.getPrivileges()[index]) {
            return true;
        }
        if (schema == null) {
            return false;
        }
        SchemaPrivDefinition schemaDef = privilegeGather.getSchemaPrivDefMap().get(schema);
        if (schemaDef != null && schemaDef.getPrivileges()[index]) {
            return true;
        }

        // Table verify
        TablePrivDefinition tableDef = privilegeGather.getTablePrivDefMap().get(table);
        if (tableDef != null && tableDef.getPrivileges()[index]) {
            return true;
        }
        return false;
    }

    public static boolean verify(String user, String host, CommonId schema, CommonId table) {
        if (!isVerify) {
            return true;
        }
        if (DingoRole.SQLLINE == env.getRole()) {
            return true;
        }
        if (StringUtils.isBlank(user)) {
            return true;
        }

        PrivilegeGather privilegeGather = env.getPrivilegeGatherMap().get(user + "#" + host);
        if (!verify(schema, table, privilegeGather)) {
            privilegeGather = env.getPrivilegeGatherMap().get(user + "#%");
            return verify(schema, table, privilegeGather);
        }
        return true;
    }

    public static boolean verify(CommonId schema, CommonId table,
                                 PrivilegeGather privilegeGather) {
        if (privilegeGather == null) {
            return false;
        }

        UserDefinition userDef = privilegeGather.getUserDef();
        if (userDef != null) {
            for (boolean privilege: userDef.getPrivileges()) {
                if (privilege) {
                    return true;
                }
            }
        }
        if (schema == null) {
            return false;
        }
        SchemaPrivDefinition schemaDef = privilegeGather.getSchemaPrivDefMap().get(schema);
        if (schemaDef != null) {
            for (boolean privilege: schemaDef.getPrivileges()) {
                if (privilege) {
                    return true;
                }
            }
        }

        if (table == null) {
            Collection<TablePrivDefinition> definitionCollection = privilegeGather.getTablePrivDefMap().values();
            return definitionCollection.stream().anyMatch(tableDef -> {
                if (schema.equals(tableDef.getSchema())) {
                    log.info("schema verify:" + schema);
                    for (boolean privilege : tableDef.getPrivileges()) {
                        if (privilege) {
                            return true;
                        }
                    }
                }
                return false;
            });
        } else {
            TablePrivDefinition tableDef = privilegeGather.getTablePrivDefMap().get(table);
            if (tableDef != null) {
                for (boolean privilege : tableDef.getPrivileges()) {
                    if (privilege) {
                        log.info("table verify result:" + true);
                        return true;
                    }
                }
            }
        }
        return false;
    }

    public static boolean verifyDuplicate(String user, String host, CommonId schema, CommonId table,
                                          List<DingoSqlAccessEnum> accessTypes) {
        if (accessTypes.stream().anyMatch(accessType -> {
            if (!verify(user, host, schema, table, accessType)) {
                return true;
            }
            return false;
        })) {
            return false;
        } else {
            return true;
        }
    }
}
