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

import io.dingodb.calcite.grammar.dql.SqlShowCreateTable;
import io.dingodb.meta.DdlService;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.IndexTable;
import io.dingodb.meta.entity.IndexType;
import io.dingodb.meta.entity.InfoSchema;
import io.dingodb.meta.entity.Table;
import lombok.Setter;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public class ShowCreateTableOperation extends QueryOperation {

    @Setter
    public SqlNode sqlNode;

    private String schemaName;
    private String tableName;

    public ShowCreateTableOperation(SqlNode sqlNode, String defaultSchemaName) {
        SqlShowCreateTable showCreateTable = (SqlShowCreateTable) sqlNode;
        SqlIdentifier tableIdentifier = showCreateTable.tableIdentifier;
        if (tableIdentifier.names.size() == 1) {
            this.schemaName = defaultSchemaName;
            tableName = tableIdentifier.names.get(0);
        } else if (tableIdentifier.names.size() == 2) {
            this.schemaName = tableIdentifier.names.get(0);
            tableName = tableIdentifier.names.get(1);
        }
    }

    @Override
    public Iterator<Object[]> getIterator() {
        List<Object[]> createTableList = new ArrayList<>();
        String createTable = getCreateTable();
        if (StringUtils.isNotBlank(createTable)) {
            Object[] tuples = new Object[]{tableName, createTable};
            createTableList.add(tuples);
        }

        return createTableList.iterator();
    }

    @Override
    public List<String> columns() {
        List<String> columns = new ArrayList<>();
        columns.add("Table");
        columns.add("Create Table");
        return columns;
    }

    private String getCreateTable() {
        InfoSchema is = DdlService.root().getIsLatest();
        Table table = is.getTable(schemaName, tableName);
        if (table == null) {
            throw new RuntimeException("Table " + tableName + " doesn't exist");
        }
        //if (table.getVersion() == 1) {
        //    return table.getCreateSql();
        //}
        StringBuilder createTableSqlStr = new StringBuilder();
        createTableSqlStr.append("CREATE ").append("TABLE ").append("`").append(tableName.toUpperCase()).append("`");
        createTableSqlStr.append("(");
        int colSize = table.getColumns().size();
        for (int i = 0; i < colSize; i ++) {
            Column column = table.getColumns().get(i);
            if (column.getName().equalsIgnoreCase("_ROWID") && column.getState() == 2) {
                createTableSqlStr.deleteCharAt(createTableSqlStr.length() - 1);
                continue;
            }
            createTableSqlStr.append("\r\n").append("    ");
            createTableSqlStr.append("`").append(column.getName()).append("` ");
            createTableSqlStr.append(getTypeName(column.getSqlTypeName(), column.getElementTypeName()));
            if (column.getPrecision() > 0) {
                createTableSqlStr.append("(").append(column.getPrecision()).append(")");
            }
            if (!column.isNullable()) {
                createTableSqlStr.append(" NOT NULL");
            }
            if (column.getDefaultValueExpr() != null) {
                createTableSqlStr.append(" DEFAULT ").append(column.getDefaultValueExpr());
            }
            if (column.isAutoIncrement()) {
                createTableSqlStr.append(" auto_increment");
            }
            if (i < colSize - 1) {
                createTableSqlStr.append(",");
            }
        }
        List<Column> keyColumnList = table.keyColumns();
        int keySize = keyColumnList.size();
        if (!(keySize == 1
            && keyColumnList.get(0).getName().equalsIgnoreCase("_ROWID")
            && keyColumnList.get(0).getState() == 2)) {
            createTableSqlStr.append(",");
            createTableSqlStr.append("\r\n");
            createTableSqlStr.append("    PRIMARY KEY (");
            for (int i = 0; i < keySize; i ++) {
                createTableSqlStr.append("`").append(keyColumnList.get(i).getName()).append("`");
                if (i < keySize - 1) {
                    createTableSqlStr.append(",");
                }
            }
            createTableSqlStr.append(")");
        }
        int indexSize = table.getIndexes().size();
        if (indexSize > 0) {
            for (int i = 0; i < indexSize; i ++) {
                createTableSqlStr.append(",");
                createTableSqlStr.append("\r\n");
                IndexTable indexTable = table.getIndexes().get(i);
                if (indexTable.getIndexType() == IndexType.SCALAR) {
                    createTableSqlStr.append("    ");
                    if (indexTable.isUnique()) {
                        createTableSqlStr.append("UNIQUE ");
                    }
                    createTableSqlStr.append("INDEX ").append(indexTable.getName());
                    createTableSqlStr.append("(");
                    int indexKeyColLen = indexTable.originKeyList.size();
                    for (int j = 0; j < indexKeyColLen; j ++) {
                        createTableSqlStr.append("`").append(indexTable.originKeyList.get(j)).append("`");
                        if (j < indexKeyColLen - 1) {
                            createTableSqlStr.append(",");
                        }
                    }
                    createTableSqlStr.append(")");
                    if (indexTable.originWithKeyList != null && !indexTable.originWithKeyList.isEmpty()) {
                        createTableSqlStr.append(" WITH(");
                        int withColSize = indexTable.originWithKeyList.size();
                        for (int ix = 0; ix < withColSize; ix ++) {
                            createTableSqlStr.append("`").append(indexTable.originWithKeyList.get(ix)).append("`");
                            if (ix < withColSize - 1) {
                                createTableSqlStr.append(",");
                            }
                        }
                        createTableSqlStr.append(")");
                    }
                } else if (indexTable.getIndexType() == IndexType.DOCUMENT) {

                } else {
                    createTableSqlStr.append("    ");
                    createTableSqlStr.append("INDEX ").append(indexTable.getName()).append(" VECTOR");
                    if (indexTable.originKeyList != null && !indexTable.originKeyList.isEmpty()) {
                        createTableSqlStr.append("(");
                        int indexKeyColLen = indexTable.originKeyList.size();
                        for (int j = 0; j < indexKeyColLen; j++) {
                            createTableSqlStr.append("`").append(indexTable.originKeyList.get(j)).append("`");
                            if (j < indexKeyColLen - 1) {
                                createTableSqlStr.append(",");
                            }
                        }
                        createTableSqlStr.append(")");
                    }
                    if (indexTable.originWithKeyList != null && !indexTable.originWithKeyList.isEmpty()) {
                        createTableSqlStr.append(" WITH(");
                        int withColSize = indexTable.originWithKeyList.size();
                        for (int j = 0; j < withColSize; j ++) {
                            createTableSqlStr.append("`").append(indexTable.originWithKeyList.get(j)).append("`");
                            if (j < withColSize - 1) {
                                createTableSqlStr.append(",");
                            }
                        }
                        createTableSqlStr.append(" ) ");
                    }
                    if (indexTable.getProperties() != null) {
                        createTableSqlStr.append(" parameters(");
                        createTableSqlStr.append("type=");
                        String type;
                        switch (indexTable.getIndexType()) {
                            case VECTOR_FLAT:
                                type = "FLAT";
                                break;
                            case VECTOR_IVF_FLAT:
                                type = "IVFFLAT";
                                break;
                            case VECTOR_IVF_PQ:
                                type = "IVFPQ";
                                break;
                            case VECTOR_HNSW:
                                type = "HNSW";
                                break;
                            case VECTOR_DISKANN:
                                type = "DISKANN";
                                break;
                            default:
                                type = "HNSW";
                                break;
                        }
                        createTableSqlStr.append(type).append(", ");
                        indexTable.getProperties().forEach((key, val) -> {
                            if (key.equals("metricType")) {
                                switch (val.toString()) {
                                    case "METRIC_TYPE_L2":
                                        val = "L2";
                                        break;
                                    case "METRIC_TYPE_COSINE":
                                        val = "COSINE";
                                        break;
                                    case "METRIC_TYPE_INNER_PRODUCT":
                                        val = "INNER_PRODUCT";
                                        break;
                                }
                            }
                            createTableSqlStr.append(key).append("=").append(val).append(", ");
                        });
                        createTableSqlStr.deleteCharAt(createTableSqlStr.length() - 2);
                        createTableSqlStr.append(") ");
                    }
                }
            }
        }
        createTableSqlStr.append("\r\n");
        createTableSqlStr.append(")");
        createTableSqlStr.append(" engine=").append(table.getEngine()).append(" ");
        createTableSqlStr.append(" replica=").append(table.getReplica());
        if (table.getPartitions().size() > 1) {
            createTableSqlStr.append(" PARTITION BY ");
            if ("hash".equalsIgnoreCase(table.getPartitionStrategy())) {
                createTableSqlStr.append("HASH PARTITIONS=").append(table.getPartitions().size());
            } else {
                createTableSqlStr.append("RANGE VALUES");
                table.getPartitions().forEach(partition -> {
                    if (Arrays.stream(partition.getOperand()).allMatch(Objects::isNull)) {
                        return;
                    }
                    createTableSqlStr.append("(");
                    int size = partition.getOperand().length;
                    for (int i = 0; i < size; i ++) {
                        if (partition.getOperand()[i] == null) {
                            break;
                        }
                        if (i  > 0) {
                            createTableSqlStr.append(",");
                        }
                        createTableSqlStr.append(partition.getOperand()[i]);
                    }
                    createTableSqlStr.append("),");
                });
                createTableSqlStr.deleteCharAt(createTableSqlStr.length() - 1);
            }
        }
        return createTableSqlStr.toString();
    }

    private static String getTypeName(String typeName, String elementTypeName) {
        switch (typeName) {
            case "INTEGER":
                return "int";
            case "STRING":
                return "varchar";
            case "ARRAY":
                return elementTypeName + " " + typeName;
            default:
                return typeName;
        }
    }
}
