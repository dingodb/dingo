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

public class ShowCollationOperation implements QueryOperation {

    private String sqlLikePattern;

    public ShowCollationOperation(String sqlLikePattern) {
        this.sqlLikePattern = sqlLikePattern;
    }

    @Override
    public Iterator getIterator() {
        List<Object[]> tupleList = new ArrayList<>();
        tupleList.add(new Object[] {"utf8_bin", "utf8", 83, "", "Yes", 1});
        return tupleList.iterator();
    }

    @Override
    public List<String> columns() {
        List<String> columns = new ArrayList<>();
        columns.add("Collation");
        columns.add("Charset");
        columns.add("Id");
        columns.add("Default");
        columns.add("Compiled");
        columns.add("Sortlen");
        return columns;
    }
}
