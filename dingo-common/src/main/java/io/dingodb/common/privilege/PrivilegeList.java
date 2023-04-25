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

package io.dingodb.common.privilege;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PrivilegeList {

    private static final List<String> userGlobalList;

    private static final List<String> schemaPrivilegeList;

    private static final List<String> tablePrivilegeList;

    public static final Map<PrivilegeType, List<String>> privilegeMap = new ConcurrentHashMap<>();

    static {
        userGlobalList = new ArrayList<>();
        schemaPrivilegeList = new ArrayList<>();
        tablePrivilegeList = new ArrayList<>();

        userGlobalList.add(DingoSqlAccessEnum.SELECT.getAccessType());
        userGlobalList.add(DingoSqlAccessEnum.INSERT.getAccessType());
        userGlobalList.add(DingoSqlAccessEnum.UPDATE.getAccessType());
        userGlobalList.add(DingoSqlAccessEnum.DELETE.getAccessType());
        userGlobalList.add(DingoSqlAccessEnum.INDEX.getAccessType());
        userGlobalList.add(DingoSqlAccessEnum.ALTER.getAccessType());
        userGlobalList.add(DingoSqlAccessEnum.CREATE.getAccessType());
        userGlobalList.add(DingoSqlAccessEnum.DROP.getAccessType());
        //userGlobalList.add("grant");
        userGlobalList.add(DingoSqlAccessEnum.CREATE_VIEW.getAccessType());
        userGlobalList.add(DingoSqlAccessEnum.SHOW_VIEW.getAccessType());
        userGlobalList.add(DingoSqlAccessEnum.CREATE_ROUTINE.getAccessType());
        userGlobalList.add(DingoSqlAccessEnum.ALTER_ROUTINE.getAccessType());
        userGlobalList.add(DingoSqlAccessEnum.EXECUTE.getAccessType());
        userGlobalList.add(DingoSqlAccessEnum.TRIGGER.getAccessType());
        userGlobalList.add(DingoSqlAccessEnum.EVENT.getAccessType());
        userGlobalList.add(DingoSqlAccessEnum.CREATE_TMP_TABLE.getAccessType());
        userGlobalList.add(DingoSqlAccessEnum.LOCK_TABLES.getAccessType());
        userGlobalList.add(DingoSqlAccessEnum.REFERENCES.getAccessType());
        userGlobalList.add(DingoSqlAccessEnum.RELOAD.getAccessType());
        userGlobalList.add(DingoSqlAccessEnum.SHUTDOWN.getAccessType());
        userGlobalList.add(DingoSqlAccessEnum.PROCESS.getAccessType());
        userGlobalList.add(DingoSqlAccessEnum.FILE.getAccessType());
        userGlobalList.add(DingoSqlAccessEnum.SHOW_DB.getAccessType());
        userGlobalList.add(DingoSqlAccessEnum.SUPER.getAccessType());
        userGlobalList.add(DingoSqlAccessEnum.REPL_SLAVE.getAccessType());
        userGlobalList.add(DingoSqlAccessEnum.REPL_CLIENT.getAccessType());
        userGlobalList.add(DingoSqlAccessEnum.CREATE_USER.getAccessType());
        userGlobalList.add(DingoSqlAccessEnum.CREATE_TABLESPACE.getAccessType());

        schemaPrivilegeList.add(DingoSqlAccessEnum.SELECT.getAccessType());
        schemaPrivilegeList.add(DingoSqlAccessEnum.INSERT.getAccessType());
        schemaPrivilegeList.add(DingoSqlAccessEnum.UPDATE.getAccessType());
        schemaPrivilegeList.add(DingoSqlAccessEnum.DELETE.getAccessType());
        schemaPrivilegeList.add(DingoSqlAccessEnum.INDEX.getAccessType());
        schemaPrivilegeList.add(DingoSqlAccessEnum.ALTER.getAccessType());
        schemaPrivilegeList.add(DingoSqlAccessEnum.CREATE.getAccessType());
        schemaPrivilegeList.add(DingoSqlAccessEnum.DROP.getAccessType());
        //schemaPrivilegeList.add("Grant");
        schemaPrivilegeList.add(DingoSqlAccessEnum.CREATE_VIEW.getAccessType());
        schemaPrivilegeList.add(DingoSqlAccessEnum.SHOW_VIEW.getAccessType());
        schemaPrivilegeList.add(DingoSqlAccessEnum.CREATE_ROUTINE.getAccessType());
        schemaPrivilegeList.add(DingoSqlAccessEnum.ALTER_ROUTINE.getAccessType());
        schemaPrivilegeList.add(DingoSqlAccessEnum.EXECUTE.getAccessType());
        schemaPrivilegeList.add(DingoSqlAccessEnum.TRIGGER.getAccessType());
        schemaPrivilegeList.add(DingoSqlAccessEnum.EVENT.getAccessType());
        schemaPrivilegeList.add(DingoSqlAccessEnum.CREATE_TMP_TABLE.getAccessType());
        schemaPrivilegeList.add(DingoSqlAccessEnum.LOCK_TABLES.getAccessType());
        schemaPrivilegeList.add(DingoSqlAccessEnum.REFERENCES.getAccessType());

        tablePrivilegeList.add(DingoSqlAccessEnum.SELECT.getAccessType());
        tablePrivilegeList.add(DingoSqlAccessEnum.INSERT.getAccessType());
        tablePrivilegeList.add(DingoSqlAccessEnum.UPDATE.getAccessType());
        tablePrivilegeList.add(DingoSqlAccessEnum.DELETE.getAccessType());
        tablePrivilegeList.add(DingoSqlAccessEnum.INDEX.getAccessType());
        tablePrivilegeList.add(DingoSqlAccessEnum.ALTER.getAccessType());
        tablePrivilegeList.add(DingoSqlAccessEnum.CREATE.getAccessType());
        tablePrivilegeList.add(DingoSqlAccessEnum.DROP.getAccessType());
        //tablePrivilegeList.add("Grant");
        tablePrivilegeList.add(DingoSqlAccessEnum.CREATE_VIEW.getAccessType());
        tablePrivilegeList.add(DingoSqlAccessEnum.SHOW_VIEW.getAccessType());
        tablePrivilegeList.add(DingoSqlAccessEnum.TRIGGER.getAccessType());
        tablePrivilegeList.add(DingoSqlAccessEnum.REFERENCES.getAccessType());

        privilegeMap.put(PrivilegeType.USER, userGlobalList);
        privilegeMap.put(PrivilegeType.SCHEMA, schemaPrivilegeList);
        privilegeMap.put(PrivilegeType.TABLE, tablePrivilegeList);
    }

}
