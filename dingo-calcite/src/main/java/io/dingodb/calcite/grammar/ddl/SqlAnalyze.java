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

package io.dingodb.calcite.grammar.ddl;

import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;
import java.util.stream.Collectors;

public class SqlAnalyze extends SqlDdl {

    private Integer cmSketchHeight = 0;
    private Integer cmSketchWidth = 0;
    private String schemaName;
    private String tableName;
    private List<String> columns;
    private Integer buckets;

    private long samples;
    private float sampleRate;

    private static final SqlOperator OPERATOR =
        new SqlSpecialOperator("ANALYZE", SqlKind.OTHER_DDL);

    /**
     * Creates a SqlDdl.
     *
     * @param pos pos
     */
    public SqlAnalyze(SqlParserPos pos,
                      SqlIdentifier tableId,
                      List<SqlIdentifier> columnList,
                      Number cmSketchHeight,
                      Number cmSketchWidth,
                      int buckets,
                      long samples,
                      float sampleRate) {
        super(OPERATOR, pos);
        if (tableId.names.size() > 1) {
            this.schemaName = tableId.names.get(0);
            this.tableName = tableId.names.get(1);
        } else {
            this.tableName = tableId.names.get(0);
        }
        if (cmSketchHeight != null) {
            this.cmSketchHeight = cmSketchHeight.intValue();
        }
        if (cmSketchWidth != null) {
            this.cmSketchWidth = cmSketchWidth.intValue();
        }
        if (columnList != null) {
            columns = columnList.stream()
                .map(sqlIdentifier -> sqlIdentifier.names.get(0).toUpperCase())
                .collect(Collectors.toList());
        }
        this.buckets = buckets;
        this.samples = samples;
        this.sampleRate = sampleRate;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return null;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("ANALYZE TABLE");
        writer.keyword("");
    }

    public Integer getCmSketchHeight() {
        return cmSketchHeight;
    }

    public Integer getCmSketchWidth() {
        return cmSketchWidth;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public List<String> getColumns() {
        return columns;
    }

    public Integer getBuckets() {
        return buckets;
    }

    public long getSamples() {
        return samples;
    }

    public float getSampleRate() {
        return sampleRate;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }
}
