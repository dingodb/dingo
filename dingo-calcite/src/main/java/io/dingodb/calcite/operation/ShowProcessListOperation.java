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

package io.dingodb.calcite.operation;

import io.dingodb.common.ProcessInfo;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ShowProcessListOperation implements QueryOperation {

    private String sqlLikePattern;

    private final boolean processPrivilege;

    private final String user;
    private final String host;

    private List<ProcessInfo> processInfoList;

    public ShowProcessListOperation(boolean processPrivilege, String user, String host) {
        this.processPrivilege = processPrivilege;
        this.user = user;
        this.host = host;
    }

    public void init(List<ProcessInfo> processInfoList) {
        this.processInfoList = processInfoList;
    }

    @Override
    public Iterator<Object[]> getIterator() {
        List<Object[]> tupleList = processInfoList
            .stream()
            .filter(processInfo -> processPrivilege
                || (!processPrivilege && processInfo.getUser().equals(user) && processInfo.getHost().equals(host)))
            .map(processInfo -> new Object[] {
            processInfo.getId(),
            processInfo.getUser(),
            processInfo.getClient(),
            processInfo.getDb(),
            processInfo.getCommand(), processInfo.getTime(), processInfo.getState(), processInfo.getInfo(),
            processInfo.getType(), processInfo.getTxnIdStr()
        }).collect(Collectors.toList());
        return tupleList.iterator();
    }

    @Override
    public List<String> columns() {List<String> columns = new ArrayList<>();
        columns.add("id");
        columns.add("User");
        columns.add("Host");
        columns.add("db");
        columns.add("Command");
        columns.add("Time");
        columns.add("State");
        columns.add("Info");
        columns.add("Type");
        columns.add("TXN");
        return columns;
    }
}
