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
import io.dingodb.expr.parser.Expr;
import io.dingodb.expr.parser.exception.DingoExprParseException;
import io.dingodb.expr.parser.parser.DingoExprCompiler;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.ContextSqlValidator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.server.DdlExecutorImpl;
import org.apache.calcite.sql.SqlBasicCall;
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
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

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
        String defaultValue = "";
        ColumnStrategy strategy = scd.strategy;
        if (strategy == ColumnStrategy.DEFAULT) {
            SqlNode expr = scd.expression;
            if (expr != null) {
                defaultValue = expr.toString();
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
    private static Pair<MutableSchema, String> getSchemaAndTableName(
        @Nonnull SqlIdentifier id,
        @Nonnull CalcitePrepare.Context context
    ) {
        CalciteSchema rootSchema = context.getMutableRootSchema();
        assert rootSchema != null : "No root schema.";
        final List<String> defaultSchemaPath = context.getDefaultSchemaPath();
        assert defaultSchemaPath.size() == 1 : "Assume that the schema path has only one level.";
        CalciteSchema defaultSchema = rootSchema.getSubSchema(defaultSchemaPath.get(0), false);
        if (defaultSchema == null) {
            defaultSchema = rootSchema;
        }
        List<String> names = new ArrayList<>(id.names);
        Schema schema;
        String tableName;
        if (names.size() == 1) {
            schema = defaultSchema.schema;
            tableName = names.get(0);
        } else {
            CalciteSchema subSchema = rootSchema.getSubSchema(names.get(0), false);
            if (subSchema != null) {
                schema = subSchema.schema;
                tableName = String.join(".", Util.skip(names));
            } else {
                schema = defaultSchema.schema;
                tableName = String.join(".", names);
            }
        }
        if (!(schema instanceof MutableSchema)) {
            throw new AssertionError("Schema must be mutable.");
        }
        return Pair.of((MutableSchema) schema, tableName);
    }

    @SuppressWarnings({"unused", "MethodMayBeStatic"})
    public void execute(SqlCreateTable create, CalcitePrepare.Context context) {
        log.info("DDL execute: {}", create);
        final Pair<MutableSchema, String> schemaTableName
            = getSchemaAndTableName(create.name, context);
        final String tableName = schemaTableName.right;
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
            throw new RuntimeException("Primary keys are required in table definition.");
        }

        long distinctColCnt = td.getColumns().stream().map(ColumnDefinition::getName).distinct().count();
        long realColCnt = td.getColumns().size();
        if (distinctColCnt != realColCnt) {
            throw new RuntimeException("Duplicate column names are not allowed in table definition. Total: "
                + realColCnt + ", distinct: " + distinctColCnt);
        }

        final MutableSchema schema = schemaTableName.left;
        assert schema != null;
        if (schema.getTable(tableName) != null) {
            if (!create.ifNotExists) {
                // They did not specify IF NOT EXISTS, so give error.
                throw SqlUtil.newContextException(
                    create.name.getParserPosition(),
                    RESOURCE.tableExists(tableName)
                );
            }
        }
        assert tableName != null;
        schema.createTable(tableName, td);
    }

    @SuppressWarnings({"unused", "MethodMayBeStatic"})
    public void execute(SqlDropTable drop, CalcitePrepare.Context context) {
        log.info("DDL execute: {}", drop);
        final Pair<MutableSchema, String> schemaTableName
            = getSchemaAndTableName(drop.name, context);
        final MutableSchema schema = schemaTableName.left;
        final String tableName = schemaTableName.right;
        final boolean existed;
        assert schema != null;
        assert tableName != null;
        existed = schema.dropTable(tableName);
        if (!existed && !drop.ifExists) {
            throw SqlUtil.newContextException(
                drop.name.getParserPosition(),
                RESOURCE.tableNotFound(drop.name.toString())
            );
        }
    }
}
