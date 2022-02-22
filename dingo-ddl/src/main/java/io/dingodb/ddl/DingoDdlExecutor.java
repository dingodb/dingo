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

package io.dingodb.ddl;

import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.common.table.TableDefinition;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.ContextSqlValidator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.server.DdlExecutorImpl;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlTypeNameSpec;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.ddl.SqlDropTable;
import org.apache.calcite.sql.ddl.SqlKeyConstraint;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static org.apache.calcite.util.Static.RESOURCE;

@Slf4j
public class DingoDdlExecutor extends DdlExecutorImpl {
    public static final DingoDdlExecutor INSTANCE = new DingoDdlExecutor();

    private DingoDdlExecutor() {
    }

    @Nullable
    private static ColumnDefinition fromSqlColumnDeclaration(
        @Nonnull SqlColumnDeclaration scd,
        SqlValidator validator,
        List<String> primaryKeyList
    ) {
        SqlDataTypeSpec typeSpec = scd.dataType;
        SqlTypeNameSpec typeNameSpec = typeSpec.getTypeNameSpec();
        if (!(typeNameSpec instanceof SqlBasicTypeNameSpec)) {
            throw SqlUtil.newContextException(
                typeNameSpec.getParserPos(),
                RESOURCE.typeNotSupported(typeNameSpec.toString())
            );
        }
        RelDataType dataType = typeSpec.deriveType(validator, true);
        SqlTypeName typeName = dataType.getSqlTypeName();
        int precision = typeName.allowsPrec() ? dataType.getPrecision() : RelDataType.PRECISION_NOT_SPECIFIED;
        int scale = typeName.allowsScale() ? dataType.getScale() : RelDataType.SCALE_NOT_SPECIFIED;
        Object defaultValue = null;
        ColumnStrategy strategy = scd.strategy;
        if (strategy == ColumnStrategy.DEFAULT) {
            SqlNode expr = scd.expression;
            if (expr instanceof SqlLiteral) {
                defaultValue = ((SqlLiteral) expr).getValue();
            }
        }
        String name = scd.name.getSimple();
        boolean isPrimary = (primaryKeyList != null && primaryKeyList.contains(name));
        return ColumnDefinition.builder()
            .name(name)
            .type(typeName)
            .precision(precision)
            .scale(scale)
            .notNull(isPrimary || !dataType.isNullable())
            .primary(isPrimary)
            .defaultValue(defaultValue)
            .build();
    }

    @Nonnull
    private static MutableSchema getSchema(@Nonnull CalcitePrepare.Context context) {
        CalciteSchema calciteSchema = context.getMutableRootSchema();
        assert calciteSchema != null : "No root schema.";
        final List<String> schemaPath = context.getDefaultSchemaPath();
        for (String p : schemaPath) {
            calciteSchema = calciteSchema.getSubSchema(p, true);
            assert calciteSchema != null : "Schema \"" + String.join(".", schemaPath) + "\"not available.";
        }
        Schema schema = calciteSchema.schema;
        if (!(schema instanceof MutableSchema)) {
            throw new AssertionError("Schema must be mutable.");
        }
        return (MutableSchema) schema;
    }

    @Nonnull
    private static String getTableName(@Nonnull SqlIdentifier id, @Nonnull CalcitePrepare.Context context) {
        final List<String> schemaPath = context.getDefaultSchemaPath();
        List<String> names = new ArrayList<>(id.names);
        for (String p : schemaPath) {
            if (names.get(0).equals(p)) {
                names.remove(p);
            } else {
                break;
            }
        }
        return String.join(".", names);
    }

    @SuppressWarnings({"unused", "MethodMayBeStatic"})
    public void execute(SqlCreateTable create, CalcitePrepare.Context context) {
        log.info("DDL execute: {}", create);
        final String tableName = getTableName(create.name, context);
        TableDefinition td = new TableDefinition(tableName);
        List<String> keyList = null;
        SqlNodeList columnList = create.columnList;
        if (columnList == null) {
            throw SqlUtil.newContextException(create.name.getParserPosition(),
                RESOURCE.createTableRequiresColumnList());
        }
        for (SqlNode sqlNode : create.columnList) {
            if (sqlNode instanceof SqlKeyConstraint) {
                SqlKeyConstraint constraint = (SqlKeyConstraint) sqlNode;
                if (constraint.getOperator().getKind() == SqlKind.PRIMARY_KEY) {
                    // The 0th element is the name of the constraint
                    keyList = ((SqlNodeList) constraint.getOperandList().get(1)).getList().stream()
                        .map(t -> ((SqlIdentifier) Objects.requireNonNull(t)).getSimple())
                        .collect(Collectors.toList());
                    break;
                }
            }
        }
        SqlValidator validator = new ContextSqlValidator(context, true);
        for (SqlNode sqlNode : create.columnList) {
            if (sqlNode.getKind() == SqlKind.COLUMN_DECL) {
                SqlColumnDeclaration scd = (SqlColumnDeclaration) sqlNode;
                ColumnDefinition cd = fromSqlColumnDeclaration(scd, validator, keyList);
                td.addColumn(cd);
            }
        }
        if (td.getColumns().stream().noneMatch(ColumnDefinition::isPrimary)) {
            throw new RuntimeException("Not have primary key!");
        }
        final MutableSchema schema = getSchema(context);
        if (schema.getTable(tableName) != null) {
            if (!create.ifNotExists) {
                // They did not specify IF NOT EXISTS, so give error.
                throw SqlUtil.newContextException(
                    create.name.getParserPosition(),
                    RESOURCE.tableExists(tableName)
                );
            }
        }
        schema.createTable(tableName, td);
    }

    @SuppressWarnings({"unused", "MethodMayBeStatic"})
    public void execute(SqlDropTable drop, CalcitePrepare.Context context) {
        log.info("DDL execute: {}", drop);
        final MutableSchema schema = getSchema(context);
        final String tableName = getTableName(drop.name, context);
        final boolean existed;
        existed = schema.dropTable(tableName);
        if (!existed && !drop.ifExists) {
            throw SqlUtil.newContextException(
                drop.name.getParserPosition(),
                RESOURCE.tableNotFound(drop.name.toString())
            );
        }
    }
}
