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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Slf4j
public class PrivilegeDict {
    public static final Map<String, Integer> privilegeIndexDict = new ConcurrentHashMap<>();

    static {
        privilegeIndexDict.put(DingoSqlAccessEnum.SELECT.getAccessType(), 1);
        privilegeIndexDict.put(DingoSqlAccessEnum.INSERT.getAccessType(), 2);
        privilegeIndexDict.put(DingoSqlAccessEnum.UPDATE.getAccessType(), 3);
        privilegeIndexDict.put(DingoSqlAccessEnum.DELETE.getAccessType(), 4);
        privilegeIndexDict.put(DingoSqlAccessEnum.INDEX.getAccessType(), 5);
        privilegeIndexDict.put(DingoSqlAccessEnum.ALTER.getAccessType(), 6);
        privilegeIndexDict.put(DingoSqlAccessEnum.CREATE.getAccessType(), 7);
        privilegeIndexDict.put(DingoSqlAccessEnum.DROP.getAccessType(), 8);
        privilegeIndexDict.put(DingoSqlAccessEnum.GRANT.getAccessType(), 9);
        privilegeIndexDict.put(DingoSqlAccessEnum.CREATE_VIEW.getAccessType(), 10);
        privilegeIndexDict.put(DingoSqlAccessEnum.SHOW_VIEW.getAccessType(), 11);
        privilegeIndexDict.put(DingoSqlAccessEnum.CREATE_ROUTINE.getAccessType(), 12);
        privilegeIndexDict.put(DingoSqlAccessEnum.ALTER_ROUTINE.getAccessType(), 13);
        privilegeIndexDict.put(DingoSqlAccessEnum.EXECUTE.getAccessType(), 14);
        privilegeIndexDict.put(DingoSqlAccessEnum.TRIGGER.getAccessType(), 15);
        privilegeIndexDict.put(DingoSqlAccessEnum.EVENT.getAccessType(), 16);
        privilegeIndexDict.put(DingoSqlAccessEnum.CREATE_TMP_TABLE.getAccessType(), 17);
        privilegeIndexDict.put(DingoSqlAccessEnum.LOCK_TABLES.getAccessType(), 18);
        privilegeIndexDict.put(DingoSqlAccessEnum.REFERENCES.getAccessType(), 19);
        privilegeIndexDict.put(DingoSqlAccessEnum.RELOAD.getAccessType(), 20);
        privilegeIndexDict.put(DingoSqlAccessEnum.SHUTDOWN.getAccessType(), 21);
        privilegeIndexDict.put(DingoSqlAccessEnum.PROCESS.getAccessType(), 22);
        privilegeIndexDict.put(DingoSqlAccessEnum.FILE.getAccessType(), 23);
        privilegeIndexDict.put(DingoSqlAccessEnum.SHOW_DB.getAccessType(), 24);
        privilegeIndexDict.put(DingoSqlAccessEnum.SUPER.getAccessType(), 25);
        privilegeIndexDict.put(DingoSqlAccessEnum.REPL_SLAVE.getAccessType(), 26);
        privilegeIndexDict.put(DingoSqlAccessEnum.REPL_CLIENT.getAccessType(), 27);
        privilegeIndexDict.put(DingoSqlAccessEnum.CREATE_USER.getAccessType(), 28);
        privilegeIndexDict.put(DingoSqlAccessEnum.CREATE_TABLESPACE.getAccessType(), 29);
        privilegeIndexDict.put(DingoSqlAccessEnum.EXTEND1.getAccessType(), 30);
        privilegeIndexDict.put(DingoSqlAccessEnum.EXTEND2.getAccessType(), 31);
        privilegeIndexDict.put(DingoSqlAccessEnum.EXTEND3.getAccessType(), 32);
        privilegeIndexDict.put(DingoSqlAccessEnum.EXTEND4.getAccessType(), 33);
        privilegeIndexDict.put(DingoSqlAccessEnum.EXTEND5.getAccessType(), 34);
    }

    public static List<String> getPrivilege(List<Integer> indexs) {
        return privilegeIndexDict.entrySet().stream().filter(entry -> indexs.contains(entry.getValue()))
            .map(Map.Entry::getKey).collect(Collectors.toList());
    }
}
