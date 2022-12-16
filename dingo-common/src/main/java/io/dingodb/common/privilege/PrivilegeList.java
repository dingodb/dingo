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

        userGlobalList.add("select");
        userGlobalList.add("insert");
        userGlobalList.add("update");
        userGlobalList.add("delete");
        userGlobalList.add("index");
        userGlobalList.add("alter");
        userGlobalList.add("create");
        userGlobalList.add("drop");
        //userGlobalList.add("grant");
        userGlobalList.add("create_view");
        userGlobalList.add("show_view");
        userGlobalList.add("create_routine");
        userGlobalList.add("alter_routine");
        userGlobalList.add("execute");
        userGlobalList.add("trigger");
        userGlobalList.add("event");
        userGlobalList.add("create_tmp_table");
        userGlobalList.add("lock_tables");
        userGlobalList.add("references");
        userGlobalList.add("reload");
        userGlobalList.add("shutdown");
        userGlobalList.add("process");
        userGlobalList.add("file");
        userGlobalList.add("show_db");
        userGlobalList.add("super");
        userGlobalList.add("repl_slave");
        userGlobalList.add("repl_client");
        userGlobalList.add("create_user");
        userGlobalList.add("create_tablespace");
        userGlobalList.add("extend1");
        userGlobalList.add("extend2");
        userGlobalList.add("extend3");
        userGlobalList.add("extend4");
        userGlobalList.add("extend5");

        schemaPrivilegeList.add("select");
        schemaPrivilegeList.add("insert");
        schemaPrivilegeList.add("update");
        schemaPrivilegeList.add("delete");
        schemaPrivilegeList.add("index");
        schemaPrivilegeList.add("alter");
        schemaPrivilegeList.add("create");
        schemaPrivilegeList.add("drop");
        //schemaPrivilegeList.add("Grant");
        schemaPrivilegeList.add("create_view");
        schemaPrivilegeList.add("show_view");
        schemaPrivilegeList.add("create_routine");
        schemaPrivilegeList.add("alter_routine");
        schemaPrivilegeList.add("execute");
        schemaPrivilegeList.add("trigger");
        schemaPrivilegeList.add("event");
        schemaPrivilegeList.add("create_tmp_table");
        schemaPrivilegeList.add("lock_tables");
        schemaPrivilegeList.add("references");

        tablePrivilegeList.add("select");
        tablePrivilegeList.add("insert");
        tablePrivilegeList.add("update");
        tablePrivilegeList.add("delete");
        tablePrivilegeList.add("index");
        tablePrivilegeList.add("alter");
        tablePrivilegeList.add("create");
        tablePrivilegeList.add("drop");
        //tablePrivilegeList.add("Grant");
        tablePrivilegeList.add("create_view");
        tablePrivilegeList.add("show_view");
        tablePrivilegeList.add("trigger");
        tablePrivilegeList.add("references");

        privilegeMap.put(PrivilegeType.USER, userGlobalList);
        privilegeMap.put(PrivilegeType.SCHEMA, schemaPrivilegeList);
        privilegeMap.put(PrivilegeType.TABLE, tablePrivilegeList);
    }

}
