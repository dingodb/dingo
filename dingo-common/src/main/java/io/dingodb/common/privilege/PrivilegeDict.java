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
        privilegeIndexDict.put("select", 1);
        privilegeIndexDict.put("insert", 2);
        privilegeIndexDict.put("update", 3);
        privilegeIndexDict.put("delete", 4);
        privilegeIndexDict.put("index", 5);
        privilegeIndexDict.put("alter", 6);
        privilegeIndexDict.put("create", 7);
        privilegeIndexDict.put("drop", 8);
        privilegeIndexDict.put("grant", 9);
        privilegeIndexDict.put("create_view", 10);
        privilegeIndexDict.put("show_view", 11);
        privilegeIndexDict.put("create_routine", 12);
        privilegeIndexDict.put("alter_routine", 13);
        privilegeIndexDict.put("execute", 14);
        privilegeIndexDict.put("trigger", 15);
        privilegeIndexDict.put("event", 16);
        privilegeIndexDict.put("create_tmp_table", 17);
        privilegeIndexDict.put("lock_tables", 18);
        privilegeIndexDict.put("references", 19);
        privilegeIndexDict.put("reload", 20);
        privilegeIndexDict.put("shutdown", 21);
        privilegeIndexDict.put("process", 22);
        privilegeIndexDict.put("file", 23);
        privilegeIndexDict.put("show_db", 24);
        privilegeIndexDict.put("super", 25);
        privilegeIndexDict.put("repl_slave", 26);
        privilegeIndexDict.put("repl_client", 27);
        privilegeIndexDict.put("create_user", 28);
        privilegeIndexDict.put("create_tablespace", 29);
        privilegeIndexDict.put("extend1", 30);
        privilegeIndexDict.put("extend2", 31);
        privilegeIndexDict.put("extend3", 32);
        privilegeIndexDict.put("extend4", 33);
        privilegeIndexDict.put("extend5", 34);
    }

    public static void reload(Object param) {
        List<String> privilegeDicts = (List<String>) param;
        for (String privilegeDict : privilegeDicts) {
            String[] privilegeMapping = privilegeDict.split("#");
            privilegeIndexDict.put(privilegeMapping[0], Integer.parseInt(privilegeMapping[1]));
        }
    }

    public static List<String> getPrivilege(List<Integer> indexs) {
        return privilegeIndexDict.entrySet().stream().filter(entry -> indexs.contains(entry.getValue()))
            .map(Map.Entry::getKey).collect(Collectors.toList());
    }
}
