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

import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Slf4j
public final class PrivilegeDict {
    public static final Map<String, Integer> privilegeIndexDict = new ConcurrentHashMap<>();

    static {
        privilegeIndexDict.put(DingoSqlAccessEnum.SELECT.getAccessType(), 0);
        privilegeIndexDict.put(DingoSqlAccessEnum.INSERT.getAccessType(), 1);
        privilegeIndexDict.put(DingoSqlAccessEnum.UPDATE.getAccessType(), 2);
        privilegeIndexDict.put(DingoSqlAccessEnum.DELETE.getAccessType(), 3);
        privilegeIndexDict.put(DingoSqlAccessEnum.INDEX.getAccessType(), 4);
        privilegeIndexDict.put(DingoSqlAccessEnum.ALTER.getAccessType(), 5);
        privilegeIndexDict.put(DingoSqlAccessEnum.CREATE.getAccessType(), 6);
        privilegeIndexDict.put(DingoSqlAccessEnum.DROP.getAccessType(), 7);
        privilegeIndexDict.put(DingoSqlAccessEnum.GRANT.getAccessType(), 8);
        privilegeIndexDict.put(DingoSqlAccessEnum.CREATE_VIEW.getAccessType(), 9);
        privilegeIndexDict.put(DingoSqlAccessEnum.SHOW_VIEW.getAccessType(), 10);
        privilegeIndexDict.put(DingoSqlAccessEnum.CREATE_ROUTINE.getAccessType(), 11);
        privilegeIndexDict.put(DingoSqlAccessEnum.ALTER_ROUTINE.getAccessType(), 12);
        privilegeIndexDict.put(DingoSqlAccessEnum.EXECUTE.getAccessType(), 13);
        privilegeIndexDict.put(DingoSqlAccessEnum.TRIGGER.getAccessType(), 14);
        privilegeIndexDict.put(DingoSqlAccessEnum.EVENT.getAccessType(), 15);
        privilegeIndexDict.put(DingoSqlAccessEnum.CREATE_TMP_TABLE.getAccessType(), 16);
        privilegeIndexDict.put(DingoSqlAccessEnum.LOCK_TABLES.getAccessType(), 17);
        privilegeIndexDict.put(DingoSqlAccessEnum.REFERENCES.getAccessType(), 18);
        privilegeIndexDict.put(DingoSqlAccessEnum.RELOAD.getAccessType(), 19);
        privilegeIndexDict.put(DingoSqlAccessEnum.SHUTDOWN.getAccessType(), 20);
        privilegeIndexDict.put(DingoSqlAccessEnum.PROCESS.getAccessType(), 21);
        privilegeIndexDict.put(DingoSqlAccessEnum.FILE.getAccessType(), 22);
        privilegeIndexDict.put(DingoSqlAccessEnum.SHOW_DB.getAccessType(), 23);
        privilegeIndexDict.put(DingoSqlAccessEnum.SUPER.getAccessType(), 24);
        privilegeIndexDict.put(DingoSqlAccessEnum.REPL_SLAVE.getAccessType(), 25);
        privilegeIndexDict.put(DingoSqlAccessEnum.REPL_CLIENT.getAccessType(), 26);
        privilegeIndexDict.put(DingoSqlAccessEnum.CREATE_USER.getAccessType(), 27);
        privilegeIndexDict.put(DingoSqlAccessEnum.CREATE_TABLESPACE.getAccessType(), 28);
        privilegeIndexDict.put(DingoSqlAccessEnum.EXTEND1.getAccessType(), 29);
        privilegeIndexDict.put(DingoSqlAccessEnum.EXTEND2.getAccessType(), 30);
        privilegeIndexDict.put(DingoSqlAccessEnum.EXTEND3.getAccessType(), 31);
        privilegeIndexDict.put(DingoSqlAccessEnum.EXTEND4.getAccessType(), 32);
        privilegeIndexDict.put(DingoSqlAccessEnum.EXTEND5.getAccessType(), 33);
    }

    private PrivilegeDict() {
    }

    public static List<String> getPrivilege(List<Integer> indexes) {
        return privilegeIndexDict.entrySet().stream().filter(entry -> indexes.contains(entry.getValue()))
            .map(Map.Entry::getKey).collect(Collectors.toList());
    }

    public static final Map<String, Integer> userPrivilegeIndex = new HashMap<>();
    public static final Map<String, Integer> dbPrivilegeIndex = new HashMap<>();

    static {
        userPrivilegeIndex.put(DingoSqlAccessEnum.SELECT.getAccessType(), 2);
        userPrivilegeIndex.put(DingoSqlAccessEnum.INSERT.getAccessType(), 3);
        userPrivilegeIndex.put(DingoSqlAccessEnum.UPDATE.getAccessType(), 4);
        userPrivilegeIndex.put(DingoSqlAccessEnum.DELETE.getAccessType(), 5);
        userPrivilegeIndex.put(DingoSqlAccessEnum.CREATE.getAccessType(), 6);
        userPrivilegeIndex.put(DingoSqlAccessEnum.DROP.getAccessType(), 7);

        userPrivilegeIndex.put(DingoSqlAccessEnum.RELOAD.getAccessType(), 8);
        userPrivilegeIndex.put(DingoSqlAccessEnum.SHUTDOWN.getAccessType(), 9);
        userPrivilegeIndex.put(DingoSqlAccessEnum.PROCESS.getAccessType(), 10);
        userPrivilegeIndex.put(DingoSqlAccessEnum.FILE.getAccessType(), 11);
        userPrivilegeIndex.put(DingoSqlAccessEnum.GRANT.getAccessType(), 12);
        userPrivilegeIndex.put(DingoSqlAccessEnum.REFERENCES.getAccessType(), 13);
        userPrivilegeIndex.put(DingoSqlAccessEnum.INDEX.getAccessType(), 14);
        userPrivilegeIndex.put(DingoSqlAccessEnum.ALTER.getAccessType(), 15);
        userPrivilegeIndex.put(DingoSqlAccessEnum.SHOW_DB.getAccessType(), 16);
        userPrivilegeIndex.put(DingoSqlAccessEnum.SUPER.getAccessType(), 17);

        userPrivilegeIndex.put(DingoSqlAccessEnum.CREATE_TMP_TABLE.getAccessType(), 18);
        userPrivilegeIndex.put(DingoSqlAccessEnum.LOCK_TABLES.getAccessType(), 19);
        userPrivilegeIndex.put(DingoSqlAccessEnum.EXECUTE.getAccessType(), 20);
        userPrivilegeIndex.put(DingoSqlAccessEnum.REPL_SLAVE.getAccessType(), 21);
        userPrivilegeIndex.put(DingoSqlAccessEnum.REPL_CLIENT.getAccessType(), 22);

        userPrivilegeIndex.put(DingoSqlAccessEnum.CREATE_VIEW.getAccessType(), 23);
        userPrivilegeIndex.put(DingoSqlAccessEnum.SHOW_VIEW.getAccessType(), 24);
        userPrivilegeIndex.put(DingoSqlAccessEnum.CREATE_ROUTINE.getAccessType(), 25);
        userPrivilegeIndex.put(DingoSqlAccessEnum.ALTER_ROUTINE.getAccessType(), 26);
        userPrivilegeIndex.put(DingoSqlAccessEnum.CREATE_USER.getAccessType(), 27);
        userPrivilegeIndex.put(DingoSqlAccessEnum.EVENT.getAccessType(), 28);
        userPrivilegeIndex.put(DingoSqlAccessEnum.TRIGGER.getAccessType(), 29);
        userPrivilegeIndex.put(DingoSqlAccessEnum.CREATE_TABLESPACE.getAccessType(), 30);

        dbPrivilegeIndex.put(DingoSqlAccessEnum.SELECT.getAccessType(), 3);
        dbPrivilegeIndex.put(DingoSqlAccessEnum.INSERT.getAccessType(), 4);
        dbPrivilegeIndex.put(DingoSqlAccessEnum.UPDATE.getAccessType(), 5);
        dbPrivilegeIndex.put(DingoSqlAccessEnum.DELETE.getAccessType(), 6);
        dbPrivilegeIndex.put(DingoSqlAccessEnum.CREATE.getAccessType(), 7);
        dbPrivilegeIndex.put(DingoSqlAccessEnum.DROP.getAccessType(), 8);
        dbPrivilegeIndex.put(DingoSqlAccessEnum.GRANT.getAccessType(), 9);
        dbPrivilegeIndex.put(DingoSqlAccessEnum.REFERENCES.getAccessType(), 10);
        dbPrivilegeIndex.put(DingoSqlAccessEnum.INDEX.getAccessType(), 11);
        dbPrivilegeIndex.put(DingoSqlAccessEnum.ALTER.getAccessType(), 12);

        dbPrivilegeIndex.put(DingoSqlAccessEnum.CREATE_TMP_TABLE.getAccessType(), 13);
        dbPrivilegeIndex.put(DingoSqlAccessEnum.LOCK_TABLES.getAccessType(), 14);
        dbPrivilegeIndex.put(DingoSqlAccessEnum.CREATE_VIEW.getAccessType(), 15);
        dbPrivilegeIndex.put(DingoSqlAccessEnum.SHOW_VIEW.getAccessType(), 16);
        dbPrivilegeIndex.put(DingoSqlAccessEnum.CREATE_ROUTINE.getAccessType(), 17);
        dbPrivilegeIndex.put(DingoSqlAccessEnum.ALTER_ROUTINE.getAccessType(), 18);
        dbPrivilegeIndex.put(DingoSqlAccessEnum.EXECUTE.getAccessType(), 19);
        dbPrivilegeIndex.put(DingoSqlAccessEnum.EVENT.getAccessType(), 20);
        dbPrivilegeIndex.put(DingoSqlAccessEnum.TRIGGER.getAccessType(), 21);
    }

}
