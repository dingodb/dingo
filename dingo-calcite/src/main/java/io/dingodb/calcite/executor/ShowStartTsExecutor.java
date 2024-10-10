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

import io.dingodb.transaction.api.TransactionService;
import io.dingodb.tso.TsoService;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ShowStartTsExecutor extends QueryExecutor {

    @Override
    Iterator<Object[]> getIterator() {
        List<Object[]> tsList = new ArrayList<>();
        tsList.add(new Object[] {TsoService.getDefault().tso()});
        return tsList.iterator();
    }

    @Override
    public List<String> columns() {
        List<String> columnList = new ArrayList<>();
        columnList.add("ts");
        return columnList;
    }
}
