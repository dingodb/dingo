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

package io.dingodb.calcite.grammar.dql;

import lombok.Getter;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

@Getter
public class SqlShowStoreJobs extends SqlShow {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator("SHOW STORE_JOBS", SqlKind.SELECT);

    private final Long jobId;
    private final Integer archiveLimit;
    private final boolean includeArchive;
    private final Long archiveStartId;

    public SqlShowStoreJobs(SqlParserPos pos) {
        this(pos, null, null, false, null);
    }

    public SqlShowStoreJobs(SqlParserPos pos, Long jobId, Integer archiveLimit,
                            boolean includeArchive, Long archiveStartId) {
        super(OPERATOR, pos);
        this.jobId = jobId;
        this.archiveLimit = archiveLimit;
        this.includeArchive = includeArchive;
        this.archiveStartId = archiveStartId;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("SHOW ");
        writer.keyword("STORE_JOBS");
    }
}
