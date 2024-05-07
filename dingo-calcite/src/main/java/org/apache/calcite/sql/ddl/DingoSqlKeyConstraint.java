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

package org.apache.calcite.sql.ddl;

import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.concurrent.atomic.AtomicInteger;

public class DingoSqlKeyConstraint extends SqlKeyConstraint {
    private static final AtomicInteger ixNu = new AtomicInteger(1);
    @Getter
    private String uniqueName;
    @Getter
    @Setter
    private boolean usePrimary;
    public DingoSqlKeyConstraint(SqlParserPos pos, @Nullable SqlIdentifier name, SqlNodeList columnList) {
        super(pos, name, columnList);
        if (name != null) {
            this.uniqueName = name.getSimple();
        } else {
            int ix = ixNu.getAndIncrement();
            this.uniqueName = "UNIQUE" + ixNu.get();
            if (ix > 100) {
                ixNu.set(1);
            }
        }
    }
}
