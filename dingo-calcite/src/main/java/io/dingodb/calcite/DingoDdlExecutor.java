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

package io.dingodb.calcite;

import com.codahale.metrics.Timer;
import io.dingodb.calcite.grammar.ddl.DingoSqlCreateTable;
import io.dingodb.calcite.grammar.ddl.SqlAlterAddColumn;
import io.dingodb.calcite.grammar.ddl.SqlAlterAddIndex;
import io.dingodb.calcite.grammar.ddl.SqlAlterConvertCharset;
import io.dingodb.calcite.grammar.ddl.SqlAlterIndex;
import io.dingodb.calcite.grammar.ddl.SqlAlterTableDistribution;
import io.dingodb.calcite.grammar.ddl.SqlAlterTenant;
import io.dingodb.calcite.grammar.ddl.SqlAlterUser;
import io.dingodb.calcite.grammar.ddl.SqlCreateIndex;
import io.dingodb.calcite.grammar.ddl.SqlCreateSchema;
import io.dingodb.calcite.grammar.ddl.SqlCreateTenant;
import io.dingodb.calcite.grammar.ddl.SqlCreateUser;
import io.dingodb.calcite.grammar.ddl.SqlDropIndex;
import io.dingodb.calcite.grammar.ddl.SqlDropTenant;
import io.dingodb.calcite.grammar.ddl.SqlDropUser;
import io.dingodb.calcite.grammar.ddl.SqlFlushPrivileges;
import io.dingodb.calcite.grammar.ddl.SqlGrant;
import io.dingodb.calcite.grammar.ddl.SqlIndexDeclaration;
import io.dingodb.calcite.grammar.ddl.SqlRevoke;
import io.dingodb.calcite.grammar.ddl.SqlSetPassword;
import io.dingodb.calcite.grammar.ddl.SqlTruncate;
import io.dingodb.calcite.grammar.ddl.SqlUseSchema;
import io.dingodb.calcite.runtime.DingoResource;
import io.dingodb.calcite.schema.RootSnapshotSchema;
import io.dingodb.calcite.schema.SubSnapshotSchema;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.meta.Tenant;
import io.dingodb.common.meta.SchemaInfo;
import io.dingodb.common.metrics.DingoMetrics;
import io.dingodb.common.partition.PartitionDefinition;
import io.dingodb.common.partition.PartitionDetailDefinition;
import io.dingodb.common.privilege.PrivilegeDefinition;
import io.dingodb.common.privilege.PrivilegeType;
import io.dingodb.common.privilege.SchemaPrivDefinition;
import io.dingodb.common.privilege.TablePrivDefinition;
import io.dingodb.common.privilege.UserDefinition;
import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.tenant.TenantConstant;
import io.dingodb.common.util.DefinitionUtils;
import io.dingodb.common.util.Optional;
import io.dingodb.common.util.Parameters;
import io.dingodb.meta.DdlService;
import io.dingodb.meta.InfoSchemaService;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.TenantService;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.IndexTable;
import io.dingodb.meta.entity.InfoSchema;
import io.dingodb.meta.entity.Table;
import io.dingodb.partition.DingoPartitionServiceProvider;
import io.dingodb.verify.plugin.AlgorithmPlugin;
import io.dingodb.verify.service.UserService;
import io.dingodb.verify.service.UserServiceProvider;
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
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlTypeNameSpec;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.ddl.DingoSqlColumn;
import org.apache.calcite.sql.ddl.DingoSqlKeyConstraint;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.ddl.SqlDropSchema;
import org.apache.calcite.sql.ddl.SqlDropTable;
import org.apache.calcite.sql.ddl.SqlKeyConstraint;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.util.Pair;
import org.apache.commons.lang3.StringUtils;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static io.dingodb.calcite.runtime.DingoResource.DINGO_RESOURCE;
import static io.dingodb.common.util.Optional.mapOrNull;
import static io.dingodb.common.util.PrivilegeUtils.getRealAddress;
import static org.apache.calcite.util.Static.RESOURCE;

@Slf4j
public class DingoDdlExecutor extends DdlExecutorImpl {
    public static final DingoDdlExecutor INSTANCE = new DingoDdlExecutor();

    private static final Pattern namePattern = Pattern.compile("^[A-Z_][A-Z\\d_]*$");

    public UserService userService;

    private DingoDdlExecutor() {
        this.userService = UserServiceProvider.getRoot();
    }

    private static List<TableDefinition> getIndexDefinitions(DingoSqlCreateTable create, TableDefinition tableDefinition) {
        assert create.columnList != null;
        List<TableDefinition> tableDefList = create.columnList.stream()
            .filter(col -> col.getKind() == SqlKind.UNIQUE)
            .filter(col -> {
                if (col instanceof DingoSqlKeyConstraint) {
                    DingoSqlKeyConstraint keyConstraint = (DingoSqlKeyConstraint) col;
                    return !keyConstraint.isUsePrimary();
                }
                return false;
            })
            .map(col -> fromSqlUniqueDeclaration((SqlKeyConstraint) col, tableDefinition))
            .collect(Collectors.toCollection(ArrayList::new));
        tableDefList.addAll(create.columnList.stream()
            .filter(col -> col.getKind() == SqlKind.CREATE_INDEX)
            .map(col -> fromSqlIndexDeclaration((SqlIndexDeclaration) col, tableDefinition))
            .collect(Collectors.toCollection(ArrayList::new)));
        long count = tableDefList.stream().map(TableDefinition::getName).distinct().count();
        if (tableDefList.size() > count) {
            throw new IllegalArgumentException("Duplicate index name");
        }
        return tableDefList;
    }

    private static @Nullable TableDefinition fromSqlUniqueDeclaration(
        @NonNull SqlKeyConstraint sqlKeyConstraint,
        TableDefinition tableDefinition
    ) {
        List<ColumnDefinition> tableColumns = tableDefinition.getColumns();
        List<String> tableColumnNames = tableColumns.stream().map(ColumnDefinition::getName)
            .collect(Collectors.toList());

        // Primary key list
        SqlNodeList sqlNodes = (SqlNodeList) sqlKeyConstraint.getOperandList().get(1);
        List<String> columns = sqlNodes.getList().stream()
            .filter(Objects::nonNull)
            .map(SqlIdentifier.class::cast)
            .map(SqlIdentifier::getSimple)
            .map(String::toUpperCase)
            .collect(Collectors.toCollection(ArrayList::new));

        Properties properties = new Properties();

        List<ColumnDefinition> indexColumnDefinitions = new ArrayList<>();
        properties.put("indexType", "scalar");
        for (int i = 0; i < columns.size(); i++) {
            String columnName = columns.get(i);
            if (!tableColumnNames.contains(columnName)) {
                throw new RuntimeException("Invalid column name: " + columnName);
            }

            ColumnDefinition columnDefinition = tableColumns.stream()
                .filter(f -> f.getName().equals(columnName))
                .findFirst().orElse(null);
            if (columnDefinition == null) {
                throw new RuntimeException("not found column");
            }

            ColumnDefinition indexColumnDefinition = ColumnDefinition.builder()
                .name(columnDefinition.getName())
                .type(columnDefinition.getTypeName())
                .elementType(columnDefinition.getElementType())
                .precision(columnDefinition.getPrecision())
                .scale(columnDefinition.getScale())
                .nullable(columnDefinition.isNullable())
                .primary(i)
                .build();
            indexColumnDefinitions.add(indexColumnDefinition);
        }
        tableDefinition.getKeyColumns().stream()
            .sorted(Comparator.comparingInt(ColumnDefinition::getPrimary))
            .map(columnDefinition -> ColumnDefinition.builder()
                .name(columnDefinition.getName())
                .type(columnDefinition.getTypeName())
                .elementType(columnDefinition.getElementType())
                .precision(columnDefinition.getPrecision())
                .scale(columnDefinition.getScale())
                .nullable(columnDefinition.isNullable())
                .primary(-1)
                .build())
            .forEach(indexColumnDefinitions::add);

        DingoSqlKeyConstraint dingoSqlKeyConstraint = (DingoSqlKeyConstraint) sqlKeyConstraint;
        TableDefinition indexTableDefinition = tableDefinition.copyWithName(dingoSqlKeyConstraint.getUniqueName());
        indexTableDefinition.setColumns(indexColumnDefinitions);
        indexTableDefinition.setProperties(properties);

        validatePartitionBy(
            indexTableDefinition.getKeyColumns().stream().map(ColumnDefinition::getName).collect(Collectors.toList()),
            indexTableDefinition,
            indexTableDefinition.getPartDefinition()
        );

        return indexTableDefinition;
    }

    private static TableDefinition fromSqlIndexDeclaration(
        @NonNull SqlIndexDeclaration indexDeclaration,
        TableDefinition tableDefinition
    ) {
        List<ColumnDefinition> tableColumns = tableDefinition.getColumns();
        List<String> tableColumnNames = tableColumns.stream().map(ColumnDefinition::getName)
            .collect(Collectors.toList());

        // Primary key list
        List<String> columns = indexDeclaration.columnList;

        int keySize = columns.size();
        tableDefinition.getKeyColumns().stream()
            .sorted(Comparator.comparingInt(ColumnDefinition::getPrimary))
            .map(ColumnDefinition::getName)
            .map(String::toUpperCase)
            .filter(__ -> !columns.contains(__))
            .forEach(columns::add);

        Properties properties = indexDeclaration.getProperties();
        if (properties == null) {
            properties = new Properties();
        }

        List<ColumnDefinition> indexColumnDefinitions = new ArrayList<>();
        boolean isScalar = true;
        if (indexDeclaration.getIndexType().equalsIgnoreCase("scalar")) {
            properties.put("indexType", "scalar");
            for (int i = 0; i < columns.size(); i++) {
                String columnName = columns.get(i);
                if (!tableColumnNames.contains(columnName)) {
                    throw new RuntimeException("Invalid column name: " + columnName);
                }

                ColumnDefinition columnDefinition = tableColumns.stream().filter(f -> f.getName().equals(columnName))
                    .findFirst().orElse(null);
                if (columnDefinition == null) {
                    throw new RuntimeException("not found column");
                }

                ColumnDefinition indexColumnDefinition = ColumnDefinition.builder()
                    .name(columnDefinition.getName())
                    .type(columnDefinition.getTypeName())
                    .elementType(columnDefinition.getElementType())
                    .precision(columnDefinition.getPrecision())
                    .scale(columnDefinition.getScale())
                    .nullable(columnDefinition.isNullable())
                    .primary(i)
                    .state(i >= keySize ? ColumnDefinition.HIDE_STATE : ColumnDefinition.NORMAL_STATE)
                    .build();
                indexColumnDefinitions.add(indexColumnDefinition);
            }
        } else if (indexDeclaration.getIndexType().equalsIgnoreCase("text")) {
            properties.put("indexType", "document");
            if (columns.size() <= 1) {
                throw new RuntimeException("Index column includes at least two columns, The first one must be text_id");
            }
            int primary = 0;
            for (int i = 0; i < columns.size(); i++) {
                String columnName = columns.get(i);
                if (!tableColumnNames.contains(columnName)) {
                    throw new RuntimeException("Invalid column name: " + columnName);
                }

                ColumnDefinition columnDefinition = tableColumns.stream().filter(f -> f.getName().equals(columnName))
                    .findFirst().get();
                if (i == 0) {
                    if (!columnDefinition.getTypeName().equals("INTEGER")
                        && !columnDefinition.getTypeName().equals("BIGINT")) {
                        throw new RuntimeException("Invalid column type: " + columnName);
                    }

                    if (columnDefinition.isNullable()) {
                        throw new RuntimeException("Column must be not null, column name: " + columnName);
                    }
                } else {
                    if (!columnDefinition.getTypeName().equals("BIGINT")
                        && !columnDefinition.getTypeName().equals("INTEGER")
                        && !columnDefinition.getTypeName().equals("DOUBLE")
                        && !columnDefinition.getTypeName().equals("VARCHAR")
                        && !columnDefinition.getTypeName().equals("STRING")
                        && !columnDefinition.getTypeName().equals("BYTES")) {
                        throw new RuntimeException("Invalid column type: " + columnDefinition.getTypeName());
                    }
                    primary = -1;
                }
                ColumnDefinition indexColumnDefinition = ColumnDefinition.builder()
                    .name(columnDefinition.getName())
                    .type(columnDefinition.getTypeName())
                    .elementType(columnDefinition.getElementType())
                    .precision(columnDefinition.getPrecision())
                    .scale(columnDefinition.getScale())
                    .nullable(columnDefinition.isNullable())
                    .primary(primary)
                    .build();
                indexColumnDefinitions.add(indexColumnDefinition);
            }
            if (properties.get("text_fields") != null) {
                String fields = String.valueOf(properties.get("text_fields"));
                fields = fields.startsWith("'") ? fields.substring(1, fields.length() - 1) : fields;
                properties.setProperty("text_fields", fields);
            }
        } else {
            properties.put("indexType", "vector");
            isScalar = false;
            int primary = 0;
            for (int i = 0; i < columns.size(); i++) {
                String columnName = columns.get(i);
                if (!tableColumnNames.contains(columnName)) {
                    throw new RuntimeException("Invalid column name: " + columnName);
                }

                ColumnDefinition columnDefinition = tableColumns.stream().filter(f -> f.getName().equals(columnName))
                    .findFirst().orElse(null);
                if (columnDefinition == null) {
                    throw new RuntimeException("not found column");
                }

                if (i == 0) {
                    if (!columnDefinition.getTypeName().equals("INTEGER")
                        && !columnDefinition.getTypeName().equals("BIGINT")) {
                        throw new RuntimeException("Invalid column type: " + columnName);
                    }

                    if (columnDefinition.isNullable()) {
                        throw new RuntimeException("Column must be not null, column name: " + columnName);
                    }
                } else if (i == 1) {
                    if (!columnDefinition.getTypeName().equals("ARRAY")
                        || !(columnDefinition.getElementType() != null
                            && columnDefinition.getElementType().equals("FLOAT"))) {
                        throw new RuntimeException("Invalid column type: " + columnName);
                    }
                    if (columnDefinition.isNullable()) {
                        throw new RuntimeException("Column must be not null, column name: " + columnName);
                    }
                    primary = -1;
                }

                ColumnDefinition indexColumnDefinition = ColumnDefinition.builder()
                    .name(columnDefinition.getName())
                    .type(columnDefinition.getTypeName())
                    .elementType(columnDefinition.getElementType())
                    .precision(columnDefinition.getPrecision())
                    .scale(columnDefinition.getScale())
                    .nullable(columnDefinition.isNullable())
                    .primary(primary)
                    .build();
                indexColumnDefinitions.add(indexColumnDefinition);
            }
        }

        // Not primary key list
        if (indexDeclaration.withColumnList != null) {
            List<String> otherColumns = indexDeclaration.withColumnList;

            for (String columnName : otherColumns) {
                // Check if the column exists in the original table
                if (!tableColumnNames.contains(columnName)) {
                    throw new RuntimeException("Invalid column name: " + columnName);
                }

                if (columns.contains(columnName)) {
                    continue;
                }

                ColumnDefinition columnDefinition = tableColumns.stream().filter(f -> f.getName().equals(columnName))
                    .findFirst().orElse(null);
                if (columnDefinition == null) {
                    throw new RuntimeException("could not find column");
                }

                ColumnDefinition indexColumnDefinition = ColumnDefinition.builder()
                    .name(columnDefinition.getName())
                    .type(columnDefinition.getTypeName())
                    .elementType(columnDefinition.getElementType())
                    .precision(columnDefinition.getPrecision())
                    .scale(columnDefinition.getScale())
                    .nullable(columnDefinition.isNullable())
                    .primary(-1)
                    .build();
                indexColumnDefinitions.add(indexColumnDefinition);
            }
        }
        TableDefinition indexTableDefinition = tableDefinition.copyWithName(indexDeclaration.index);
        indexTableDefinition.setColumns(indexColumnDefinitions);
        indexTableDefinition.setPartDefinition(indexDeclaration.getPartDefinition());
        int replica = getReplica(indexDeclaration.getReplica(), isScalar);
        indexTableDefinition.setReplica(replica);
        indexTableDefinition.setProperties(properties);
        indexTableDefinition.setEngine(indexDeclaration.getEngine());

        validatePartitionBy(
            indexTableDefinition.getKeyColumns().stream().map(ColumnDefinition::getName).collect(Collectors.toList()),
            indexTableDefinition,
            indexTableDefinition.getPartDefinition()
        );

        return indexTableDefinition;
    }

    private static @Nullable ColumnDefinition fromSqlColumnDeclaration(
        @NonNull DingoSqlColumn scd,
        SqlValidator validator,
        List<String> pkSet
    ) {
        SqlDataTypeSpec typeSpec = scd.dataType;
        RelDataType dataType = typeSpec.deriveType(validator, true);
        SqlTypeName typeName = dataType.getSqlTypeName();
        int precision = typeName.allowsPrec() ? dataType.getPrecision() : RelDataType.PRECISION_NOT_SPECIFIED;
        if ((typeName == SqlTypeName.TIME || typeName == SqlTypeName.TIMESTAMP)) {
            if (precision > 3) {
                throw new RuntimeException("Precision " + precision + " is not support.");
            }
        }
        String defaultValue = null;
        ColumnStrategy strategy = scd.strategy;
        if (strategy == ColumnStrategy.DEFAULT) {
            SqlNode expr = scd.expression;
            if (expr != null) {
                defaultValue = expr.toSqlString(c -> c.withDialect(AnsiSqlDialect.DEFAULT)
                    .withAlwaysUseParentheses(false)
                    .withSelectListItemsOnSeparateLines(false)
                    .withUpdateSetListNewline(false)
                    .withIndentation(0)
                    .withQuoteAllIdentifiers(false)
                ).getSql();
            }
        }

        String name = scd.name.getSimple().toUpperCase();
        if (!namePattern.matcher(name).matches()) {
            throw DINGO_RESOURCE.invalidColumn().ex();
        }

        // Obtaining id from method
        if (scd.isAutoIncrement() && !SqlTypeName.INT_TYPES.contains(typeName)) {
            throw DINGO_RESOURCE.specifierForColumn(name).ex();
        }

        if (!dataType.isNullable() && "NULL".equalsIgnoreCase(defaultValue)) {
            throw DINGO_RESOURCE.invalidDefaultValue(name).ex();
        }
        assert pkSet != null;
        int primary = pkSet.indexOf(name);
        int scale = typeName.allowsScale() ? dataType.getScale() : RelDataType.SCALE_NOT_SPECIFIED;
        RelDataType elementType = dataType.getComponentType();
        SqlTypeName elementTypeName = elementType != null ? elementType.getSqlTypeName() : null;
        return ColumnDefinition.builder()
            .name(name)
            .type(typeName.getName())
            .elementType(mapOrNull(elementTypeName, SqlTypeName::getName))
            .precision(precision)
            .scale(scale)
            .nullable(!(primary >= 0) && dataType.isNullable())
            .primary(primary)
            .defaultValue(defaultValue)
            .autoIncrement(scd.isAutoIncrement())
            .comment(scd.getComment())
            .build();
    }

    private static @Nullable ColumnDefinition createRowIdColDef(
        SqlValidator validator
    ) {
        SqlTypeNameSpec sqlTypeNameSpec = new SqlBasicTypeNameSpec(SqlTypeName.BIGINT, -1,
            new SqlParserPos(0, 0));
        SqlDataTypeSpec typeSpec = new SqlDataTypeSpec(sqlTypeNameSpec, new SqlParserPos(10, 1));
        RelDataType dataType = typeSpec.deriveType(validator, true);
        SqlTypeName typeName = dataType.getSqlTypeName();

        String name = "_ROWID";

        String defaultValue = "";

        int scale = typeName.allowsScale() ? dataType.getScale() : RelDataType.SCALE_NOT_SPECIFIED;
        RelDataType elementType = dataType.getComponentType();
        SqlTypeName elementTypeName = elementType != null ? elementType.getSqlTypeName() : null;
        return ColumnDefinition.builder()
            .name(name)
            .type(typeName.getName())
            .elementType(mapOrNull(elementTypeName, SqlTypeName::getName))
            .precision(-1)
            .scale(scale)
            .nullable(false)
            .primary(0)
            .defaultValue(defaultValue)
            .autoIncrement(true)
            // hide column
            .state(2)
            .build();
    }

    private static SubSnapshotSchema getSnapShotSchema(
        @NonNull SqlIdentifier id, CalcitePrepare.@NonNull Context context, boolean ifExist
    ) {
        CalciteSchema rootSchema = context.getMutableRootSchema();
        assert rootSchema != null : "No root schema.";

        List<String> names = new ArrayList<>(id.names);
        Schema schema = null;

        if (names.size() == 1) {
            final List<String> defaultSchemaPath = context.getDefaultSchemaPath();
            assert defaultSchemaPath.size() == 1 : "Assume that the schema path has only one level.";
            schema = Optional.mapOrNull(rootSchema.getSubSchema(defaultSchemaPath.get(0), false), $ -> $.schema);
        } else {
            CalciteSchema subSchema = rootSchema.getSubSchema(names.get(0), false);
            if (subSchema != null) {
                schema = subSchema.schema;
            } else if (!ifExist) {
                throw DINGO_RESOURCE.unknownSchema(names.get(0)).ex();
            }
        }
        return (SubSnapshotSchema) schema;
    }

    private static @NonNull String getTableName(@NonNull SqlIdentifier id) {
        if (id.names.size() == 1) {
            return id.names.get(0).toUpperCase();
        } else {
            return id.names.get(1).toUpperCase();
        }
    }

    private static @NonNull Pair<SubSnapshotSchema, String> getSchemaAndTableName(
        @NonNull SqlIdentifier id, CalcitePrepare.@NonNull Context context
    ) {
        return Pair.of(getSnapShotSchema(id, context, false), getTableName(id));
    }

    public void execute(SqlCreateSchema schema, CalcitePrepare.Context context) {
        final Timer.Context timeCtx = DingoMetrics.getTimeContext("createSchema");
        LogUtils.info(log, "DDL execute: {}", schema);
        String connId = (String) context.getDataContext().get("connId");
        RootSnapshotSchema rootSchema = (RootSnapshotSchema) context.getMutableRootSchema().schema;
        String schemaName = schema.name.names.get(0).toUpperCase();
        if (rootSchema.getSubSchema(schemaName) == null) {
            DdlService ddlService = DdlService.root();
            ddlService.createSchema(schemaName, connId);
        } else if (!schema.ifNotExists) {
            throw SqlUtil.newContextException(
                schema.name.getParserPosition(),
                RESOURCE.schemaExists(schemaName)
            );
        }
        timeCtx.stop();
    }

    public void execute(SqlDropSchema schema, CalcitePrepare.Context context) {
        final Timer.Context timeCtx = DingoMetrics.getTimeContext("dropSchema");
        LogUtils.info(log, "DDL execute: {}", schema);
        String connId = (String) context.getDataContext().get("connId");
        RootSnapshotSchema rootSchema = (RootSnapshotSchema) context.getMutableRootSchema().schema;
        String schemaName = schema.name.names.get(0).toUpperCase();
        if (schemaName.equalsIgnoreCase(context.getDefaultSchemaPath().get(0))) {
            throw new RuntimeException("Schema used.");
        }
        SubSnapshotSchema subSchema = rootSchema.getSubSchema(schemaName);
        if (subSchema == null) {
            if (!schema.ifExists) {
                InfoSchema is = rootSchema.getIs();
                LogUtils.error(log, "drop schema but get null, name:"
                    + schemaName + ", info schema is null:" + (is == null));
                if (is != null) {
                    String schemaStr = String.join(",", is.getSchemaMap().keySet());
                    LogUtils.error(log, "drop schema but get null, name:"
                        + schemaName + ", is schemaMap:" + schemaStr + ", is ver:" + is.getSchemaMetaVersion());
                }
                throw SqlUtil.newContextException(
                    schema.name.getParserPosition(),
                    RESOURCE.schemaNotFound(schemaName)
                );
            } else {
                return;
            }
        }
        if (subSchema.getTableNames().isEmpty()) {
            SchemaInfo schemaInfo = subSchema.getSchemaInfo(schemaName);
            DdlService ddlService = DdlService.root();
            ddlService.dropSchema(schemaInfo, connId);
        } else {
            throw new RuntimeException("Schema not empty.");
        }
        timeCtx.stop();
    }

    public void execute(SqlCreateTenant createT, CalcitePrepare.Context context) {
        final Timer.Context timeCtx = DingoMetrics.getTimeContext("createTenant");
        LogUtils.info(log, "DDL execute: {}", createT);
        TenantService tenantService = TenantService.getDefault();
        if (TenantConstant.TENANT_ID != 0) {
            throw new RuntimeException("Regular tenants are unable to create tenant.");
        }
        if (tenantService.getTenant(createT.name) == null) {
            Tenant tenant = Tenant.builder()
                .name(createT.name)
                .remarks(createT.remarks)
                .createdTime(System.currentTimeMillis())
                .build();
            tenantService.createTenant(tenant);
        } else if (!createT.ifNotExists) {
            throw SqlUtil.newContextException(
                createT.getParserPosition(),
                DINGO_RESOURCE.tenantExists(createT.name)
            );
        }
        timeCtx.stop();
    }

    public void execute(@NonNull SqlAlterTenant sqlAlterTenant, CalcitePrepare.Context context) {
        final Timer.Context timeCtx = DingoMetrics.getTimeContext("alterTenant");
        LogUtils.info(log, "DDL execute: {}", sqlAlterTenant);
        TenantService tenantService = TenantService.getDefault();
        if (TenantConstant.TENANT_ID != 0) {
            throw new RuntimeException("Regular tenants are unable to alter tenant.");
        }
        if (tenantService.getTenant(sqlAlterTenant.oldName) != null) {
            tenantService.updateTenant(sqlAlterTenant.oldName, sqlAlterTenant.newName);
        }

        timeCtx.stop();
    }

    public void execute(SqlDropTenant tenant, CalcitePrepare.Context context) {
        final Timer.Context timeCtx = DingoMetrics.getTimeContext("dropTenant");
        LogUtils.info(log, "DDL execute: {}", tenant);
        TenantService tenantService = TenantService.getDefault();
        if (TenantConstant.TENANT_ID != 0) {
            throw new RuntimeException("Regular tenants are unable to drop tenant");
        }
        if (tenantService.getTenant(tenant.name) == null) {
            if (!tenant.ifExists) {
                throw SqlUtil.newContextException(
                    tenant.getParserPosition(),
                    DINGO_RESOURCE.tenantNotFound(tenant.name)
                );
            } else {
                return;
            }
        }
        tenantService.dropTenant(tenant.name);
        timeCtx.stop();
    }

    @SuppressWarnings({"unused"})
    public void execute(SqlCreateTable createT, CalcitePrepare.Context context) {
        final Timer.Context timeCtx = DingoMetrics.getTimeContext("createTable");
        long start = System.currentTimeMillis();
        DingoSqlCreateTable create = (DingoSqlCreateTable) createT;
        LogUtils.info(log, "DDL execute: {}", create);
        String connId = (String) context.getDataContext().get("connId");
        SubSnapshotSchema schema = getSnapShotSchema(create.name, context, false);
        SqlNodeList columnList = create.columnList;
        if (columnList == null) {
            throw SqlUtil.newContextException(create.name.getParserPosition(),
                RESOURCE.createTableRequiresColumnList());
        }
        final String tableName = getTableName(create.name);

        // Get all primary key
        List<String> pks = create.columnList.stream()
            .filter(SqlKeyConstraint.class::isInstance)
            .map(SqlKeyConstraint.class::cast)
            .filter(constraint -> constraint.getOperator().getKind() == SqlKind.PRIMARY_KEY)
            .findAny()
            // The 0th element is the name of the constraint
            .map(constraint -> (SqlNodeList) constraint.getOperandList().get(1))
            .map(sqlNodes -> sqlNodes.getList().stream()
                .filter(Objects::nonNull)
                .map(SqlIdentifier.class::cast)
                .map(SqlIdentifier::getSimple)
                .map(String::toUpperCase)
                .collect(Collectors.toCollection(ArrayList::new))
            ).filter(ks -> !ks.isEmpty()).orElseGet(() -> create.columnList.stream()
                .filter(DingoSqlColumn.class::isInstance)
                .map(DingoSqlColumn.class::cast)
                .filter(DingoSqlColumn::isPrimaryKey)
                .map(column -> column.name.getSimple())
                .map(String::toUpperCase)
                .collect(Collectors.toCollection(ArrayList::new)));
        if (pks.isEmpty()) {
            pks = create.columnList.stream()
                .filter(SqlKeyConstraint.class::isInstance)
                .map(SqlKeyConstraint.class::cast)
                .filter(constraint -> constraint.getOperator().getKind() == SqlKind.UNIQUE)
                .findFirst()
                .map(key -> {
                    DingoSqlKeyConstraint sqlKeyConstraint = (DingoSqlKeyConstraint) key;
                    sqlKeyConstraint.setUsePrimary(true);
                    return  (SqlNodeList) sqlKeyConstraint.getOperandList().get(1);
                }).map(sqlNodes -> sqlNodes.getList().stream()
                    .filter(Objects::nonNull)
                    .map(SqlIdentifier.class::cast)
                    .map(SqlIdentifier::getSimple)
                    .map(String::toUpperCase)
                    .collect(Collectors.toCollection(ArrayList::new))
                ).orElseGet(ArrayList::new);

        }

        SqlValidator validator = new ContextSqlValidator(context, true);

        // Mapping, column node -> column definition
        List<String> finalPks = pks;
        List<ColumnDefinition> columns = create.columnList.stream()
            .filter(col -> col.getKind() == SqlKind.COLUMN_DECL)
            .map(col -> fromSqlColumnDeclaration((DingoSqlColumn) col, validator, finalPks))
            .collect(Collectors.toCollection(ArrayList::new));
        // If it is a table without a primary key, create an invisible column _rowid is a self increasing primary key
        if (pks.isEmpty()) {
            pks.add("_ROWID");
            columns.add(createRowIdColDef(validator));
        }

        // Check if specified primary keys are in column list.
        List<String> cols = columns.stream().map(ColumnDefinition::getName).collect(Collectors.toList());
        for (String pkName : pks) {
            if (!cols.contains(pkName)) {
                throw DINGO_RESOURCE.primaryKeyNotExist(pkName, tableName).ex();
            }
        }

        // Distinct column
        long distinctColCnt = columns.stream().map(ColumnDefinition::getName).distinct().count();
        long realColCnt = columns.size();
        if (distinctColCnt != realColCnt) {
            throw DINGO_RESOURCE.duplicateColumn().ex();
        }

        // Check table exist
        if (schema.getTable(tableName) != null) {
            if (!create.ifNotExists) {
                // They did not specify IF NOT EXISTS, so give error.
                throw SqlUtil.newContextException(
                    create.name.getParserPosition(),
                    RESOURCE.tableExists(tableName)
                );
            } else {
                return;
            }
        }
        int replica = getReplica(create.getReplica(), true);

        TableDefinition tableDefinition = TableDefinition.builder()
            .name(tableName)
            .columns(columns)
            .version(1)
            .ttl(create.getTtl())
            .partDefinition(create.getPartDefinition())
            .engine(create.getEngine())
            .properties(create.getProperties())
            .autoIncrement(create.getAutoIncrement())
            .replica(replica)
            .createSql(create.getOriginalCreateSql())
            .comment(create.getComment())
            .charset(create.getCharset())
            .collate(create.getCollate())
            .tableType("BASE TABLE")
            .rowFormat("Dynamic")
            .createTime(System.currentTimeMillis())
            .updateTime(0)
            .build();

        List<TableDefinition> indexTableDefinitions = getIndexDefinitions(create, tableDefinition);

        // Validate partition strategy
        validatePartitionBy(pks, tableDefinition, tableDefinition.getPartDefinition());

        if (!indexTableDefinitions.isEmpty()) {
            if (isNotTxnEngine(tableDefinition.getEngine())) {
                throw new IllegalArgumentException("Table with index, the engine must be transactional.");
            }
            indexTableDefinitions.stream()
                .filter(index -> isNotTxnEngine(index.getEngine()))
                .findAny().ifPresent(index -> {
                    throw new IllegalArgumentException("Index [" + index.getName() + "] engine must be transactional.");
                });
        }

        tableDefinition.setIndices(indexTableDefinitions);
        DdlService ddlService = DdlService.root();
        ddlService.createTableWithInfo(schema.getSchemaName(), tableName, tableDefinition, connId, create.getOriginalCreateSql());
        timeCtx.stop();
        long end = System.currentTimeMillis();
        long cost = end - start;
        if (cost > 10000) {
            LogUtils.info(log, "[ddl] create table take long time, schemaName:{} tableName:{}", schema.getSchemaName(), tableName);
        }
    }

    private static boolean isNotTxnEngine(String engine) {
        return engine != null && !engine.isEmpty() && !engine.toUpperCase().startsWith("TXN");
    }

    @SuppressWarnings({"unused", "MethodMayBeStatic"})
    public void execute(SqlDropTable drop, CalcitePrepare.Context context) throws Exception {
        final Timer.Context timeCtx = DingoMetrics.getTimeContext("dropTable");
        long start = System.currentTimeMillis();
        LogUtils.info(log, "DDL execute: {}", drop);
        String connId = (String) context.getDataContext().get("connId");
        final SubSnapshotSchema schema = getSnapShotSchema(drop.name, context, drop.ifExists);
        if (schema == null) {
            return;
        }
        final String tableName = getTableName(drop.name);
        SchemaInfo schemaInfo = schema.getSchemaInfo(schema.getSchemaName());
        Table table = schema.getTableInfo(tableName);
        if (table == null) {
            if (drop.ifExists) {
                return;
            } else {
                throw DINGO_RESOURCE.unknownTable(schema.getSchemaName() + "." + tableName).ex();
            }
        }
        DdlService ddlService = DdlService.root();
        ddlService.dropTable(schemaInfo, table.tableId.seq, tableName, connId);
        timeCtx.stop();
        long cost = System.currentTimeMillis() - start;
        if (cost > 10000) {
            LogUtils.info(log, "drop table take long time, schemaName:{}, tableName:{}", schemaInfo.getName(), tableName);
        }
    }

    public void execute(@NonNull SqlTruncate truncate, CalcitePrepare.Context context) throws Exception {
        long start = System.currentTimeMillis();
        final Timer.Context timeCtx = DingoMetrics.getTimeContext("truncateTable");
        LogUtils.info(log, "DDL execute: {}", truncate);
        String connId = (String) context.getDataContext().get("connId");
        SqlIdentifier name = (SqlIdentifier) truncate.getOperandList().get(0);
        final Pair<SubSnapshotSchema, String> schemaTableName
            = getSchemaAndTableName(name, context);
        final SubSnapshotSchema schema = Parameters.nonNull(schemaTableName.left, "table schema");
        SchemaInfo schemaInfo = schema.getSchemaInfo(schema.getSchemaName());
        String tableName = Parameters.nonNull(schemaTableName.right, "table name").toUpperCase();
        Table table = schema.getTableInfo(tableName);
        if (table == null) {
            throw DINGO_RESOURCE.tableNotExists(tableName).ex();
        }
        DdlService ddlService = DdlService.root();
        ddlService.truncateTable(schemaInfo, table, connId);
        timeCtx.stop();
        long cost = System.currentTimeMillis() - start;
        if (cost > 10000) {
            LogUtils.info(log, "truncate table cost long time, schemaName:{}, tableName:{}", schemaInfo.getName(), tableName);
        }
    }

    public void execute(@NonNull SqlGrant sqlGrant, CalcitePrepare.Context context) {
        LogUtils.info(log, "DDL execute: {}", sqlGrant);
        if (!"*".equals(sqlGrant.table)) {
            SqlIdentifier name = sqlGrant.tableIdentifier;
            final Pair<SubSnapshotSchema, String> schemaTableName
                = getSchemaAndTableName(name, context);
            final String tableName = Parameters.nonNull(schemaTableName.right, "table name");
            final SubSnapshotSchema schema = Parameters.nonNull(schemaTableName.left, "table schema");
            if (schema.getTable(tableName) == null) {
                throw new RuntimeException("table doesn't exist");
            }
        }
        sqlGrant.host = getRealAddress(sqlGrant.host);
        if (userService.existsUser(UserDefinition.builder().user(sqlGrant.user)
            .host(sqlGrant.host).build())) {
            PrivilegeDefinition privilegeDefinition = getPrivilegeDefinition(sqlGrant, context);
            String grantorUser = (String) context.getDataContext().get("user");
            String grantorHost = (String) context.getDataContext().get("host");
            privilegeDefinition.setGrantorUser(grantorUser);
            privilegeDefinition.setGrantorHost(grantorHost);
            if (sqlGrant.withGrantOption) {
                privilegeDefinition.getPrivilegeList().add("grant");
            }
            userService.grant(privilegeDefinition);
        } else {
            throw new RuntimeException("You are not allowed to create a user with GRANT");
        }
    }

    public void execute(@NonNull SqlAlterAddColumn sqlAlterAddColumn, CalcitePrepare.Context context) {
        final Pair<SubSnapshotSchema, String> schemaTableName
            = getSchemaAndTableName(sqlAlterAddColumn.table, context);
        final String tableName = Parameters.nonNull(schemaTableName.right, "table name");
        final SubSnapshotSchema schema = Parameters.nonNull(schemaTableName.left, "table schema");
        Table definition = schema.getTableInfo(tableName);
        ColumnDefinition newColumn = fromSqlColumnDeclaration(
            (DingoSqlColumn) sqlAlterAddColumn.getColumnDeclaration(),
            new ContextSqlValidator(context, true),
            definition.keyColumns().stream().map(Column::getName).collect(Collectors.toList())
        );
        List<Column> columns = new ArrayList<>(definition.getColumns());

        assert newColumn != null;
        if (definition.getColumn(newColumn.getName()) == null) {
            throw new RuntimeException();
        }
        Column column = Column.builder()
            .name(newColumn.getName())
            .primaryKeyIndex(newColumn.getPrimary())
            .type(newColumn.getType())
            .scale(newColumn.getScale())
            .precision(newColumn.getPrecision())
            .state(newColumn.getState())
            .autoIncrement(newColumn.isAutoIncrement())
            .defaultValueExpr(newColumn.getDefaultValue())
            .elementTypeName(newColumn.getElementType())
            .comment(newColumn.getComment())
            .build();

        columns.add(column);
        definition.copyWithColumns(columns);
        // todo
        //schema.updateTable(tableName, definition);
    }

    public void execute(@NonNull SqlRevoke sqlRevoke, CalcitePrepare.Context context) {
        LogUtils.info(log, "DDL execute: {}", sqlRevoke);
        sqlRevoke.host = getRealAddress(sqlRevoke.host);
        if (userService.existsUser(UserDefinition.builder().user(sqlRevoke.user)
            .host(sqlRevoke.host).build())) {
            PrivilegeDefinition privilegeDefinition = getPrivilegeDefinition(sqlRevoke, context);
            userService.revoke(privilegeDefinition);
        } else {
            throw new RuntimeException("You are not allowed to create a user with GRANT");
        }
    }

    public void execute(@NonNull SqlCreateUser sqlCreateUser, CalcitePrepare.Context context) {
        LogUtils.info(log, "DDL execute: {}", sqlCreateUser.toLog());
        UserDefinition userDefinition = UserDefinition.builder().user(sqlCreateUser.user)
            .host(getRealAddress(sqlCreateUser.host)).build();
        if (userService.existsUser(userDefinition)) {
            throw DINGO_RESOURCE.createUserFailed(sqlCreateUser.user, sqlCreateUser.host).ex();
        } else {
            userDefinition.setPlugin(sqlCreateUser.plugin);
            if ("dingo_ldap".equalsIgnoreCase(sqlCreateUser.plugin) && StringUtils.isNoneBlank(sqlCreateUser.pluginDn)) {
                userDefinition.setLdapUser(sqlCreateUser.pluginDn);
            }
            userDefinition.setRequireSsl(sqlCreateUser.requireSsl);
            userDefinition.setLock(sqlCreateUser.lock);
            String digestPwd = AlgorithmPlugin.digestAlgorithm(sqlCreateUser.password, userDefinition.getPlugin());
            userDefinition.setPassword(digestPwd);
            userDefinition.setExpireDays(sqlCreateUser.expireDays);
            userService.createUser(userDefinition);
        }
    }

    public void execute(@NonNull SqlDropUser sqlDropUser, CalcitePrepare.Context context) {
        LogUtils.info(log, "DDL execute: {}", sqlDropUser);
        UserDefinition userDefinition = UserDefinition.builder().user(sqlDropUser.name)
            .host(getRealAddress(sqlDropUser.host)).build();
        if (!userService.existsUser(userDefinition) && !sqlDropUser.ifExists) {
            throw DINGO_RESOURCE.dropUserFailed(sqlDropUser.name, sqlDropUser.host).ex();
        }
        userService.dropUser(userDefinition);
    }

    public void execute(@NonNull SqlFlushPrivileges dingoFlushPrivileges, CalcitePrepare.Context context) {
        userService.flushPrivileges();
    }

    public void execute(@NonNull SqlSetPassword sqlSetPassword, CalcitePrepare.Context context) {
        UserDefinition userDefinition = UserDefinition.builder()
            .user(sqlSetPassword.user)
            .host(getRealAddress(sqlSetPassword.host))
            .build();
        if (userService.existsUser(userDefinition)) {
            userDefinition.setPassword(sqlSetPassword.password);
            userService.updateUser(userDefinition);
        } else {
            throw DINGO_RESOURCE.noMatchingRowForUser().ex();
        }
    }

    public void execute(@NonNull SqlAlterTableDistribution sqlAlterTableDistribution, CalcitePrepare.Context context) {
        final Timer.Context timeCtx = DingoMetrics.getTimeContext("alter_table_distribution");
        final Pair<SubSnapshotSchema, String> schemaTableName
            = getSchemaAndTableName(sqlAlterTableDistribution.table, context);
        final String tableName = Parameters.nonNull(schemaTableName.right, "table name");
        SubSnapshotSchema schema = Parameters.nonNull(schemaTableName.left, "table schema");
        PartitionDetailDefinition detail = sqlAlterTableDistribution.getPartitionDefinition();

        Table table = schema.getTableInfo(tableName);
        List<Column> keyColumns = table.columns.stream()
            .filter(Column::isPrimary)
            .sorted(Comparator.comparingInt(Column::getPrimaryKeyIndex))
            .collect(Collectors.toList());
        try {
            DefinitionUtils.checkAndConvertRangePartition(
                keyColumns.stream().map(Column::getName).collect(Collectors.toList()),
                Collections.emptyList(),
                keyColumns.stream().map(Column::getType).collect(Collectors.toList()),
                Collections.singletonList(detail.getOperand())
            );
        } catch (Exception e) {
            throw DINGO_RESOURCE.illegalArgumentException().ex();
        }
        LogUtils.info(log, "DDL execute SqlAlterTableDistribution tableName: {}, partitionDefinition: {}", tableName, detail);
        MetaService metaService = MetaService.root();
        metaService.addDistribution(schema.getSchemaName(), tableName, sqlAlterTableDistribution.getPartitionDefinition());
        timeCtx.stop();
    }

    private static TableDefinition fromTable(Table table) {
        return TableDefinition.builder()
            .name(table.name)
            .tableType(table.tableType)
            .updateTime(table.updateTime)
            .version(table.version)
            .engine(table.engine)
            .autoIncrement(table.autoIncrement)
            .charset(table.charset)
            .createSql(table.createSql)
            .replica(table.replica)
            .rowFormat(table.rowFormat)
            .createTime(table.createTime)
            .comment(table.comment)
            .collate(table.collate)
            .columns(table.columns.stream().map(DingoDdlExecutor::fromColumn).collect(Collectors.toList()))
            .properties(table.properties)
            .build();
    }

    private static ColumnDefinition fromColumn(Column column) {
        return ColumnDefinition.builder()
            .name(column.name)
            .type(column.sqlTypeName)
            .state(column.state)
            .autoIncrement(column.autoIncrement)
            .elementType(column.elementTypeName)
            .comment(column.comment)
            .defaultValue(column.defaultValueExpr)
            .nullable(column.isNullable())
            .precision(column.precision)
            .primary(column.primaryKeyIndex)
            .scale(column.scale)
            .build();
    }

    public void execute(@NonNull SqlAlterAddIndex sqlAlterAddIndex, CalcitePrepare.Context context) {
        final Pair<SubSnapshotSchema, String> schemaTableName
            = getSchemaAndTableName(sqlAlterAddIndex.table, context);
        final String tableName = Parameters.nonNull(schemaTableName.right, "table name");
        final SubSnapshotSchema schema = Parameters.nonNull(schemaTableName.left, "table schema");
        DingoTable table = schema.getTable(tableName);
        if (table == null) {
            throw new RuntimeException("table not exists");
        }
        TableDefinition indexDef = fromSqlIndexDeclaration(
            sqlAlterAddIndex.getIndexDeclaration(), fromTable(table.getTable())
        );
        validateIndex(schema, tableName, indexDef);
        DdlService ddlService = DdlService.root();
        ddlService.createIndex(schema.getSchemaName() , tableName, indexDef);
    }

    public void execute(@NonNull SqlAlterIndex sqlAlterIndex, CalcitePrepare.Context context) {
        final Pair<SubSnapshotSchema, String> schemaTableName
            = getSchemaAndTableName(sqlAlterIndex.table, context);
        final String tableName = Parameters.nonNull(schemaTableName.right, "table name");
        final SubSnapshotSchema schema = Parameters.nonNull(schemaTableName.left, "table schema");
        DingoTable table = schema.getTable(tableName);
        assert table != null;
        IndexTable indexTable = table.getIndexDefinition(sqlAlterIndex.getIndex());
        if (indexTable == null) {
            throw new RuntimeException("The index " + sqlAlterIndex.getIndex() + " not exist.");
        }
        if (sqlAlterIndex.getProperties().contains("indexType")) {
            throw new IllegalArgumentException("Cannot change index type.");
        }
        indexTable.getProperties().putAll(sqlAlterIndex.getProperties());
    }

    public void execute(@NonNull SqlCreateIndex sqlCreateIndex, CalcitePrepare.Context context) {
        final Pair<SubSnapshotSchema, String> schemaTableName
            = getSchemaAndTableName(sqlCreateIndex.table, context);
        final String tableName = Parameters.nonNull(schemaTableName.right, "table name");
        final SubSnapshotSchema schema = Parameters.nonNull(schemaTableName.left, "table schema");
        DingoTable table = schema.getTable(tableName);
        if (table == null) {
            throw new RuntimeException("table not exists");
        }
        SqlIndexDeclaration sqlIndexDeclaration = new SqlIndexDeclaration(
            sqlCreateIndex.getParserPosition(),
            sqlCreateIndex.index,
            sqlCreateIndex.columns,
            null,
            null,
            null,
            0,
            "scalar",
            "TXN_LSM"
        );
        TableDefinition indexDef = fromSqlIndexDeclaration(sqlIndexDeclaration, fromTable(table.getTable()));
        validateIndex(schema, tableName, indexDef);
        DdlService.root().createIndex(schema.getSchemaName(), tableName, indexDef);
    }

    public void execute(@NonNull SqlDropIndex sqlDropIndex, CalcitePrepare.Context context) {
        final Pair<SubSnapshotSchema, String> schemaTableName
            = getSchemaAndTableName(sqlDropIndex.table, context);
        final String tableName = Parameters.nonNull(schemaTableName.right, "table name");
        final SubSnapshotSchema schema = Parameters.nonNull(schemaTableName.left, "table schema");
        validateDropIndex(schema, tableName, sqlDropIndex.index);
        DdlService.root().dropIndex(schema.getSchemaName(), tableName, sqlDropIndex.index);
    }

    public static void validateDropIndex(SubSnapshotSchema schema, String tableName, String indexName) {
        DingoTable table = schema.getTable(tableName);
        if (table == null) {
            throw new IllegalArgumentException("table " + tableName + " does not exist");
        }
        if (table.getIndexTableDefinitions().stream().map(IndexTable::getName).noneMatch(indexName::equalsIgnoreCase)) {
            throw new RuntimeException("The index " + indexName + " not exist.");
        }
    }

    public static void validateIndex(SubSnapshotSchema schema, String tableName, TableDefinition index) {
        DingoTable table = schema.getTable(tableName);
        if (table == null) {
            throw new IllegalArgumentException("table " + tableName + " does not exist ");
        }
        String indexName = index.getName();

        for (IndexTable existIndex : table.getIndexTableDefinitions()) {
            String name = existIndex.getName();
            if (indexName.equalsIgnoreCase(name)) {
                throw new RuntimeException("The index " + indexName + " already exist.");
            }
            List<String> existIndexColumns = existIndex.getColumns().stream()
                .map(Column::getName)
                .sorted()
                .collect(Collectors.toList());
            List<String> newIndexColumns = index.getColumns().stream()
                .map(ColumnDefinition::getName)
                .sorted()
                .collect(Collectors.toList());
            if (existIndexColumns.equals(newIndexColumns)) {
                throw new RuntimeException("The index columns same of " + existIndex.getName());
            }

            if ("vector".equalsIgnoreCase(index.getProperties().getProperty("indexType"))
                && existIndex.getColumns().get(1).equals(index.getColumn(1))) {
                throw new RuntimeException("The vector index column same of " + existIndex.getName());
            }
        }
    }

    public void execute(@NonNull SqlAlterUser sqlAlterUser, CalcitePrepare.Context context) {
        final Timer.Context timeCtx = DingoMetrics.getTimeContext("alterUser");
        LogUtils.info(log, "DDL execute: {}", sqlAlterUser.toLog());
        UserDefinition userDefinition = UserDefinition.builder()
            .user(sqlAlterUser.user)
            .host(getRealAddress(sqlAlterUser.host))
            .build();
        if (userService.existsUser(userDefinition)) {
            userDefinition.setPassword(sqlAlterUser.password);
            userDefinition.setRequireSsl(sqlAlterUser.requireSsl);
            userDefinition.setLock(sqlAlterUser.lock);
            userDefinition.setExpireDays(sqlAlterUser.expireDays);
            userService.updateUser(userDefinition);
        } else {
            throw DINGO_RESOURCE.alterUserFailed(sqlAlterUser.user, sqlAlterUser.host).ex();
        }
        timeCtx.stop();
    }

    public void execute(SqlAlterConvertCharset sqlAlterConvert, CalcitePrepare.Context context) {
        // alter table charset collate
    }

    public void execute(@NonNull SqlUseSchema sqlUseSchema, CalcitePrepare.Context context) {
        // for example use mysql
    }

    public static void validatePartitionBy(
        @NonNull List<String> keyList,
        @NonNull TableDefinition tableDefinition,
        PartitionDefinition partDefinition
    ) {
        long incCount = tableDefinition.getColumns()
            .stream()
            .filter(ColumnDefinition::isAutoIncrement)
            .count();
        if (incCount > 1) {
            throw DINGO_RESOURCE.multiAutoInc().ex();
        }
        if (partDefinition == null) {
            partDefinition = new PartitionDefinition();
            tableDefinition.setPartDefinition(partDefinition);
            partDefinition.setFuncName(DingoPartitionServiceProvider.RANGE_FUNC_NAME);
            partDefinition.setColumns(keyList);
            partDefinition.setDetails(new ArrayList<>());
        }
    }

    @NonNull
    private static PrivilegeDefinition getPrivilegeDefinition(
        @NonNull SqlGrant sqlGrant, CalcitePrepare.@NonNull Context context
    ) {
        LogUtils.info(log, "DDL execute: {}", sqlGrant);
        String tableName = sqlGrant.table;
        String schemaName = sqlGrant.schema;
        PrivilegeDefinition privilegeDefinition;
        PrivilegeType privilegeType;
        if ("*".equals(tableName) && "*".equals(schemaName)) {
            privilegeDefinition = UserDefinition.builder()
                .build();
            privilegeType = PrivilegeType.USER;
        } else if ("*".equals(tableName)) {
            // todo: current version, ignore name case
            if (context.getRootSchema().getSubSchema(schemaName, false) == null) {
                throw new RuntimeException("schema " + schemaName + " does not exist");
            }
            privilegeDefinition = SchemaPrivDefinition.builder()
                .schemaName(schemaName)
                .build();
            privilegeType = PrivilegeType.SCHEMA;
        } else {
            // todo: current version, ignore name case
            CalciteSchema schema = context.getRootSchema().getSubSchema(schemaName, false);
            if (schema == null) {
                throw new RuntimeException("schema " + schemaName + " does not exist");
            }
            if (schema.getTable(tableName, true) == null) {
                throw new RuntimeException("table " + tableName + " does not exist");
            }
            privilegeDefinition = TablePrivDefinition.builder()
                .schemaName(schemaName)
                .tableName(tableName)
                .build();
            privilegeType = PrivilegeType.TABLE;
        }
        privilegeDefinition.setPrivilegeList(sqlGrant.getPrivileges(privilegeType));
        privilegeDefinition.setUser(sqlGrant.user);
        privilegeDefinition.setHost(sqlGrant.host);
        return privilegeDefinition;
    }

    public static int getReplica(int targetReplica, boolean isScalar) {
        int replica;
        if (isScalar) {
            replica = InfoSchemaService.root().getStoreReplica();
            if (targetReplica == 0) {
                return replica;
            }
            if (targetReplica > 0 && replica < targetReplica) {
                throw DINGO_RESOURCE.notEnoughRegion().ex();
            }
        } else {
            replica = InfoSchemaService.root().getIndexReplica();
            if (targetReplica == 0) {
                return replica;
            }
            if (targetReplica > 0 && replica < targetReplica) {
                throw DINGO_RESOURCE.notEnoughRegion().ex();
            }
        }
        return targetReplica;
    }

}
