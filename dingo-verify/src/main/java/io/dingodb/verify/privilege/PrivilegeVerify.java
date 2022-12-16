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
import io.dingodb.common.auth.DingoRole;
import io.dingodb.common.domain.Domain;
import io.dingodb.common.privilege.PrivilegeDefinition;
import io.dingodb.common.privilege.PrivilegeDict;
import io.dingodb.common.privilege.PrivilegeGather;
import io.dingodb.common.privilege.SchemaPrivDefinition;
import io.dingodb.common.privilege.TablePrivDefinition;
import io.dingodb.common.privilege.UserDefinition;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class PrivilegeVerify {

    public static boolean isVerify = true;

    public static boolean matchHost(PrivilegeDefinition privilegeDefinition, String host) {
        if ("%".equals(privilegeDefinition.getHost()) || host.equals(privilegeDefinition.getHost())) {
            return true;
        } else {
            return false;
        }
    }

    public PrivilegeVerify() {
        this(true);
    }

    public PrivilegeVerify(boolean isVerify) {
        this.isVerify = isVerify;
    }

    public static UserDefinition matchUser(String host, List<UserDefinition> userDefList) {
        List<UserDefinition> userDefs = userDefList.stream()
            .filter(userDefinition -> matchHost(userDefinition, host)).collect(Collectors.toList());

        UserDefinition userDef = null;
        if (userDefs.size() > 0) {
            userDef = userDefs.get(0);
        }
        return userDef;
    }

    public static boolean verify(String user, String host, CommonId schema, CommonId table,
                             String accessType) {
        if (!isVerify) {
            return true;
        }
        if (DingoRole.SQLLINE == Domain.role) {
            return true;
        }
        if (StringUtils.isBlank(user)) {
            return true;
        }

        PrivilegeGather privilegeGather = Domain.INSTANCE.privilegeGatherMap.get(user + "#" + host);
        if (privilegeGather == null) {
            privilegeGather = Domain.INSTANCE.privilegeGatherMap.get(user + "#%");
            if (privilegeGather == null) {
                return false;
            }
        }

        log.info(" privilege for {} @ {} detail:" + privilegeGather, user, host);
        Integer index = PrivilegeDict.privilegeIndexDict.get(accessType);
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

    public static boolean verifyDuplicate(String user, String host, CommonId schema, CommonId table,
                                 List<String> accessTypes) {
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
