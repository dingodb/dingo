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

package io.dingodb.calcite.executor;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ShowTriggersExecutor  extends QueryExecutor {
    String sqlLikePattern;
    public ShowTriggersExecutor(String sqlLikePattern) {
        this.sqlLikePattern = sqlLikePattern;
    }

    @Override
    Iterator<Object[]> getIterator() {
        List<Object[]> tableStatus = new ArrayList<>();
        return tableStatus.iterator();
    }

    @Override
    public List<String> columns() {
        List<String> columns = new ArrayList<>();
        columns.add("Trigger");
        columns.add("Event");
        columns.add("Table");
        columns.add("Statement");
        columns.add("Timing");
        columns.add("Created");
        columns.add("sql_mode");
        columns.add("Definer");
        columns.add("character_set_client");
        columns.add("collation_connection");
        columns.add("Database Collation");
        return columns;
    }
}
