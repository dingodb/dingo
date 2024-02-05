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

import io.dingodb.common.auth.Authentication;
import io.dingodb.common.auth.DingoRole;
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.common.config.SecurityConfiguration;
import io.dingodb.common.environment.ExecutionEnvironment;
import io.dingodb.common.privilege.DingoSqlAccessEnum;
import io.dingodb.common.privilege.PrivilegeDict;
import io.dingodb.common.privilege.PrivilegeGather;
import io.dingodb.common.privilege.SchemaPrivDefinition;
import io.dingodb.common.privilege.TablePrivDefinition;
import io.dingodb.common.privilege.UserDefinition;
import io.dingodb.common.util.PrivilegeUtils;
import io.dingodb.net.Channel;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@Slf4j
public final class PrivilegeVerify {

    static ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    static {
        isVerify = SecurityConfiguration.isVerify();
    }

    private final static List<Integer> filterPx = new ArrayList<Integer>();
    static {
        // select
        filterPx.add(0);
        // insert
        filterPx.add(1);
        // update
        filterPx.add(2);
        // delete
        filterPx.add(3);
        // index
        filterPx.add(4);
        // alter
        filterPx.add(5);
        // create
        filterPx.add(6);
        // drop
        filterPx.add(7);
    }

    public static boolean isVerify;

    private PrivilegeVerify() {
    }

    public static boolean verify(Channel channel, String schema, String table,
                                 DingoSqlAccessEnum accessType) {
        Object[] objects = channel.auth().get("token");
        if (objects == null) {
            throw new IllegalArgumentException("Access denied, invalid parameter.");
        }
        Authentication authentication = (Authentication) objects[0];
        String user = authentication.getUsername();
        String host = authentication.getHost();
        return verify(user, host, schema, table, accessType);
    }

    public static boolean verify(String user, String host, String schema, String table,
                                 DingoSqlAccessEnum accessType) {
        if (prefilter(user) || verifyInformationSchema(schema, accessType)) {
            return true;
        }
        PrivilegeGather privilegeGather = env.getPrivilegeGatherMap().get(user + "#"
            + PrivilegeUtils.getRealAddress(host));
        if (privilegeGather == null) {
            privilegeGather = env.getPrivilegeGatherMap().get(user + "#%");
            if (privilegeGather == null) {
                privilegeGather = env.getPrivilegeGatherMap().get(user + "#" + DingoConfiguration.host());
                if (privilegeGather == null) {
                    return false;
                }
            }
        }
        return verify(user, host, schema, table, accessType, privilegeGather);
    }

    public static boolean verifyInformationSchema(String schema, DingoSqlAccessEnum accessType) {
        if ("INFORMATION_SCHEMA".equalsIgnoreCase(schema) && accessType == DingoSqlAccessEnum.SELECT) {
            return true;
        } else {
            return false;
        }
    }

    public static boolean verify(String user, String host, String schema, String table,
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
        SchemaPrivDefinition schemaDef = privilegeGather.getSchemaPrivDefMap().get(schema.toUpperCase());
        if (schemaDef != null && schemaDef.getPrivileges()[index]) {
            return true;
        }

        // Table verify
        TablePrivDefinition tableDef = privilegeGather.getTablePrivDefMap().get(table);
        return tableDef != null && tableDef.getPrivileges()[index];
    }

    public static boolean verify(String user, String host, String schema, String table, String command) {
        if (prefilter(user) || verifyInformationSchema(schema, DingoSqlAccessEnum.SELECT)) return true;

        PrivilegeGather privilegeGather = env.getPrivilegeGatherMap().get(user + "#"
            + PrivilegeUtils.getRealAddress(host));
        if (privilegeGather == null) {
            privilegeGather = env.getPrivilegeGatherMap().get(user + "#%");
            if (privilegeGather == null) {
                privilegeGather = env.getPrivilegeGatherMap().get(user + "#" + DingoConfiguration.host());
                if (privilegeGather == null) {
                    return false;
                }
            }
        }
        return verify(schema, table, privilegeGather, command);
    }

    private static boolean prefilter(String user) {
        if (!isVerify) {
            return true;
        }
        if (DingoRole.SQLLINE == env.getRole()) {
            return true;
        }
        return StringUtils.isBlank(user);
    }

    public static boolean verify(String schema, String table,
                                 PrivilegeGather privilegeGather, String command) {
        if (privilegeGather == null) {
            return false;
        }

        UserDefinition userDef = privilegeGather.getUserDef();
        if (userDef != null) {
            for (int i = 0; i < userDef.getPrivileges().length; i ++) {
                boolean privilege = userDef.getPrivileges()[i];
                if (privilege && isFilter(command, i)) {
                    return true;
                }
            }
        }
        if (schema == null) {
            return false;
        }

        SchemaPrivDefinition schemaDef = privilegeGather.getSchemaPrivDefMap().get(schema);
        if (schemaDef != null) {
            for (int i = 0; i < schemaDef.getPrivileges().length; i ++) {
                boolean privilege = schemaDef.getPrivileges()[i];
                if (privilege && isFilter(command, i)) {
                    return true;
                }
            }
        }

        if (table == null) {
            Collection<TablePrivDefinition> definitionCollection = privilegeGather.getTablePrivDefMap().values();
            return definitionCollection.stream().anyMatch(tableDef -> {
                if (schema.equalsIgnoreCase(tableDef.getSchemaName())) {
                    log.info("schema verify:" + schema);
                    for (int i = 0; i < tableDef.getPrivileges().length; i ++) {
                        boolean privilege = tableDef.getPrivileges()[i];
                        if (privilege && isFilter(command, i)) {
                            return true;
                        }
                    }
                }
                return false;
            });
        } else {
            TablePrivDefinition tableDef = privilegeGather.getTablePrivDefMap().get(table);
            if (tableDef != null) {
                for (int i = 0; i < tableDef.getPrivileges().length; i ++) {
                    boolean privilege = tableDef.getPrivileges()[i];
                    if (privilege && isFilter(command, i)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private static boolean isFilter(String command, int i) {
        boolean filter = false;
        if ("use".equals(command) || "getTables".equals(command)) {
            filter = filterPx.contains(i);
        } else if ("getSchemas".equals(command)) {
            // 23 means show databases
            filter = filterPx.contains(i) || i == 23;
        }
        return filter;
    }

    public static boolean verifyDuplicate(String user, String host, String schema, String table,
                                          List<DingoSqlAccessEnum> accessTypes) {
        return accessTypes.stream().allMatch(accessType -> verify(user, host, schema, table, accessType));
    }
}
