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

package io.dingodb.test.asserts;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;

import static org.assertj.core.api.Assertions.assertThat;

public final class AssertSqlNode extends Assert<SqlNode, AssertSqlNode> {
    AssertSqlNode(SqlNode obj) {
        super(obj);
    }

    public AssertSqlNode kind(SqlKind kind) {
        assertThat(instance).isNotNull();
        assertThat(instance.getKind()).isEqualTo(kind);
        return this;
    }

    @SuppressWarnings("UnusedReturnValue")
    public AssertSqlNode isTableName(String... names) {
        kind(SqlKind.IDENTIFIER);
        SqlIdentifier tbl = (SqlIdentifier) instance;
        assertThat(tbl.names).hasSameSizeAs(names);
        for (int i = 0; i < names.length; ++i) {
            assertThat(tbl.names).element(i).isEqualTo(names[i]);
        }
        return this;
    }
}
