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

import com.google.common.collect.ImmutableList;
import io.dingodb.calcite.DingoParserContext;

import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class ShowWarningsOperation implements QueryOperation {
    private DingoParserContext context;

    public ShowWarningsOperation(DingoParserContext context) {
        this.context = context;
    }

    @Override
    public Iterator<Object[]> getIterator() {
        List<SQLWarning> warningList = context.getWarningList();
        if (warningList == null) {
            List<Object[]> res = new ArrayList<>();
            return res.iterator();
        }
        List<Object[]> res = warningList
            .stream()
            .map(sqlWarning -> new Object[]{"Error", sqlWarning.getErrorCode(), sqlWarning.getMessage()})
            .collect(Collectors.toList());
        return res.iterator();
    }

    @Override
    public List<String> columns() {
        List<String> columns = new ArrayList<>();
        columns.add("level");
        columns.add("code");
        columns.add("message");
        return columns;
    }
}
