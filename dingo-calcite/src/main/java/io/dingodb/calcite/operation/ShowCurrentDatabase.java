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

import io.dingodb.calcite.DingoParserContext;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ShowCurrentDatabase implements QueryOperation {

    DingoParserContext context;

    public ShowCurrentDatabase(DingoParserContext context) {
        this.context = context;
    }

    @Override
    public Iterator getIterator() {
        List<Object[]> schemas = new ArrayList<>();
        String usedSchema = "";
        if (context.getUsedSchema() != null) {
            usedSchema = context.getUsedSchema().getName();
        }
        if (StringUtils.isNotBlank(usedSchema)) {
            Object[] tuples = new Object[]{usedSchema};
            schemas.add(tuples);
        }
        return schemas.iterator();
    }

    @Override
    public List<String> columns() {
        List<String> columns = new ArrayList<>();
        columns.add("database()");
        return columns;
    }
}
