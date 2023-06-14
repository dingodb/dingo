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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ShowTableStatusOperation implements QueryOperation {

    String schema;
    String sqlLikePattern;

    public ShowTableStatusOperation(String schema, String sqlLikePattern) {
        this.schema = schema;
        this.sqlLikePattern = sqlLikePattern;
    }

    @Override
    public Iterator getIterator() {
        List<Object[]> tableStatus = new ArrayList<>();
        return tableStatus.iterator();
    }

    @Override
    public List<String> columns() {
        List<String> columns = new ArrayList<>();
        columns.add("Name");
        columns.add("Engine");
        columns.add("Version");
        columns.add("Row_format");
        columns.add("Rows");
        columns.add("Avg_row_length");
        columns.add("Data_length");
        columns.add("Max_data_length");
        columns.add("Index_length");
        columns.add("Data_free");
        columns.add("Auto_increment");
        columns.add("Create_time");
        columns.add("Update_time");
        columns.add("Check_time");
        columns.add("Collation");
        columns.add("Checksum");
        columns.add("Create_options");
        columns.add("Comment");
        return columns;
    }
}
