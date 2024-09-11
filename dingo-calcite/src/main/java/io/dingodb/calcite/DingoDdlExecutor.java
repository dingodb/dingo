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
import io.dingodb.calcite.grammar.ddl.SqlAlterDropColumn;
import io.dingodb.calcite.grammar.ddl.SqlAlterDropIndex;
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
import io.dingodb.calcite.schema.RootCalciteSchema;
import io.dingodb.calcite.schema.RootSnapshotSchema;
import io.dingodb.calcite.schema.SubCalciteSchema;
import io.dingodb.calcite.schema.SubSnapshotSchema;
import io.dingodb.common.ddl.ActionType;
import io.dingodb.common.ddl.DdlUtil;
import io.dingodb.common.ddl.SchemaDiff;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.meta.SchemaState;
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
import io.dingodb.common.table.IndexDefinition;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.tenant.TenantConstant;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.ListType;
import io.dingodb.common.type.MapType;
import io.dingodb.common.type.scalar.BooleanType;
import io.dingodb.common.type.scalar.DateType;
import io.dingodb.common.type.scalar.DecimalType;
import io.dingodb.common.type.scalar.DoubleType;
import io.dingodb.common.type.scalar.FloatType;
import io.dingodb.common.type.scalar.IntegerType;
import io.dingodb.common.type.scalar.LongType;
import io.dingodb.common.type.scalar.StringType;
import io.dingodb.common.type.scalar.TimeType;
import io.dingodb.common.type.scalar.TimestampType;
import io.dingodb.common.util.DefinitionUtils;
import io.dingodb.common.util.Optional;
import io.dingodb.common.util.Parameters;
import io.dingodb.expr.runtime.utils.DateTimeUtils;
import io.dingodb.meta.DdlService;
import io.dingodb.meta.InfoSchemaService;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.TenantService;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.IndexTable;
import io.dingodb.meta.entity.InfoSchema;
import io.dingodb.meta.entity.SchemaTables;
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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
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

    private static List<IndexDefinition> getIndexDefinitions(DingoSqlCreateTable create, TableDefinition tableDefinition) {
        assert create.columnList != null;
        List<IndexDefinition> tableDefList = create.columnList.stream()
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

    private static @Nullable IndexDefinition fromSqlUniqueDeclaration(
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
        IndexDefinition indexTableDefinition = IndexDefinition.createIndexDefinition(
            dingoSqlKeyConstraint.getUniqueName(), tableDefinition, true, columns, null
        );
        //TableDefinition indexTableDefinition = tableDefinition.copyWithName(dingoSqlKeyConstraint.getUniqueName());
        indexTableDefinition.setColumns(indexColumnDefinitions);
        indexTableDefinition.setProperties(properties);

        validatePartitionBy(
            indexTableDefinition.getKeyColumns().stream().map(ColumnDefinition::getName).collect(Collectors.toList()),
            indexTableDefinition,
            indexTableDefinition.getPartDefinition()
        );

        return indexTableDefinition;
    }

    private static IndexDefinition fromSqlIndexDeclaration(
        @NonNull SqlIndexDeclaration indexDeclaration,
        TableDefinition tableDefinition
    ) {
        List<ColumnDefinition> tableColumns = tableDefinition.getColumns();
        List<String> tableColumnNames = tableColumns.stream().map(ColumnDefinition::getName)
            .collect(Collectors.toList());

        // Primary key list
        List<String> columns = indexDeclaration.columnList;
        List<String> originKeyList = new ArrayList<>(columns);

        int keySize = columns.size();
        List<ColumnDefinition> keyColumns = tableDefinition.getKeyColumns();
        AtomicInteger num = new AtomicInteger(0);
        keyColumns.stream()
            .sorted(Comparator.comparingInt(ColumnDefinition::getPrimary))
            .map(ColumnDefinition::getName)
            .map(String::toUpperCase)
            .peek(__ -> { if (columns.contains(__)) num.getAndIncrement(); })
            .filter(__ -> !columns.contains(__))
            .forEach(columns::add);

        Properties properties = indexDeclaration.getProperties();
        if (properties == null) {
            properties = new Properties();
        }

        List<ColumnDefinition> indexColumnDefinitions = new ArrayList<>();
        //boolean isScalar = true;
        int type = 1;
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
            String errorMsg = "Index column includes at least two columns, The first one must be text_id";
            if (num.get() > 0) {
                if (columns.size() < 2) {
                    throw new RuntimeException(errorMsg);
                }
            } else {
                if (columns.size() <= 2) {
                    throw new RuntimeException(errorMsg);
                }
            }
            type = 3;
            int primary = 0;
            for (int i = 0; i < columns.size(); i++) {
                String columnName = columns.get(i);
                if (!tableColumnNames.contains(columnName)) {
                    throw new RuntimeException("Invalid column name: " + columnName);
                }

                ColumnDefinition columnDefinition = tableColumns.stream().filter(f -> f.getName().equals(columnName))
                    .findFirst().orElseThrow(() -> new RuntimeException("not found column"));
                if (i == 0) {
                    if (!columnDefinition.getTypeName().equals("LONG")
                        && !columnDefinition.getTypeName().equals("BIGINT")) {
                        throw new RuntimeException("Invalid column type: " + columnName);
                    }

                    if (columnDefinition.isNullable()) {
                        throw new RuntimeException("Column must be not null, column name: " + columnName);
                    }
                } else {
                    if (!columnDefinition.getTypeName().equals("BIGINT")
                        && !columnDefinition.getTypeName().equals("LONG")
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
            } else {
                throw new RuntimeException("Invalid parameters");
            }
        } else {
            properties.put("indexType", "vector");
            type = 2;
            int primary = 0;
            for (int i = 0; i < columns.size(); i++) {
                String columnName = columns.get(i);
                if (!tableColumnNames.contains(columnName)) {
                    throw new RuntimeException("Invalid column name: " + columnName);
                }

                ColumnDefinition columnDefinition = tableColumns.stream().filter(f -> f.getName().equals(columnName))
                    .findFirst().orElseThrow(() -> new RuntimeException("not found column"));

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
        IndexDefinition indexTableDefinition = IndexDefinition.createIndexDefinition(
            indexDeclaration.index, tableDefinition, indexDeclaration.unique, originKeyList, indexDeclaration.withColumnList
        );
        indexTableDefinition.setColumns(indexColumnDefinitions);
        indexTableDefinition.setPartDefinition(indexDeclaration.getPartDefinition());
        int replica = getReplica(indexDeclaration.getReplica(), type);
        indexTableDefinition.setReplica(replica);
        indexTableDefinition.setProperties(properties);
        String engine = null;
        if (indexDeclaration.getEngine() != null) {
            engine = indexDeclaration.getEngine().toUpperCase();
        }
        indexTableDefinition.setEngine(engine);

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
        long schemaId = 0;
        if (rootSchema.getSubSchema(schemaName) == null) {
            InfoSchemaService infoSchemaService = InfoSchemaService.root();
            schemaId = infoSchemaService.genSchemaId();
            DdlService ddlService = DdlService.root();
            ddlService.createSchema(schemaName, schemaId, connId);
        } else if (!schema.ifNotExists) {
            throw SqlUtil.newContextException(
                schema.name.getParserPosition(),
                RESOURCE.schemaExists(schemaName)
            );
        }

        RootCalciteSchema rootCalciteSchema = (RootCalciteSchema) context.getMutableRootSchema();
        RootSnapshotSchema rootSnapshotSchema = (RootSnapshotSchema) rootCalciteSchema.schema;
        rootSnapshotSchema.applyDiff(SchemaDiff.builder().schemaId(schemaId)
            .type(ActionType.ActionCreateSchema).build());
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
        long schemaId = 0;
        if (subSchema.getTableNames().isEmpty()) {
            SchemaInfo schemaInfo = subSchema.getSchemaInfo(schemaName);
            schemaId = schemaInfo.getSchemaId();
            DdlService ddlService = DdlService.root();
            ddlService.dropSchema(schemaInfo, connId);
        } else {
            throw new RuntimeException("Schema not empty.");
        }
        RootCalciteSchema rootCalciteSchema = (RootCalciteSchema) context.getMutableRootSchema();
        RootSnapshotSchema rootSnapshotSchema = (RootSnapshotSchema) rootCalciteSchema.schema;
        rootSnapshotSchema.applyDiff(SchemaDiff.builder().schemaId(schemaId)
            .type(ActionType.ActionDropSchema).build());
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
        if (tenant.name.equalsIgnoreCase("root")) {
            throw new RuntimeException("The default tenant cannot be drop");
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

        InfoSchemaService infoSchemaService = InfoSchemaService.root();
        Long tenantId = tenantService.getTenantId(tenant.name);
        if (tenant.purgeResources) {
            InfoSchema infoSchema = infoSchemaService.getInfoSchemaByTenantId(tenantId);
            for (Map.Entry<String, SchemaTables> entry : infoSchema.schemaMap.entrySet()) {
                MetaService metaService = MetaService.root();
                SchemaTables schemaTables = entry.getValue();
                if (schemaTables.getSchemaInfo() == null) {
                    throw DINGO_RESOURCE.unknownSchema(entry.getKey()).ex();
                }
                for (Map.Entry<String, Table> tableEntry : schemaTables.getTables().entrySet()) {
                    metaService.dropTable(tenantId, schemaTables.getSchemaInfo().getSchemaId(), tableEntry.getKey());
                }
            }
        } else {
            InfoSchema infoSchema = infoSchemaService.getInfoSchemaByTenantId(tenantId);
            List<String> schemas = Arrays.asList("MYSQL", "META", "INFORMATION_SCHEMA");
            for (Map.Entry<String, SchemaTables> entry : infoSchema.schemaMap.entrySet()) {
                if (schemas.contains(entry.getKey())) {
                    continue;
                }
                if (!entry.getValue().getTables().isEmpty()) {
                    throw new RuntimeException("Tenants cannot be deleted, tables need to be cleared first");
                }
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
        LogUtils.info(log, "DDL execute: {}", create.getOriginalCreateSql().toUpperCase());
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
        if (schema == null) {
            if (context.getDefaultSchemaPath() != null && !context.getDefaultSchemaPath().isEmpty()) {
                throw DINGO_RESOURCE.unknownSchema(context.getDefaultSchemaPath().get(0)).ex();
            } else {
                throw DINGO_RESOURCE.unknownSchema("DINGO").ex();
            }
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
        int replica = getReplica(create.getReplica(), 1);

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

        List<IndexDefinition> indexTableDefinitions = getIndexDefinitions(create, tableDefinition);

        // Validate partition strategy
        validatePartitionBy(pks, tableDefinition, tableDefinition.getPartDefinition());

        validateEngine(indexTableDefinitions, tableDefinition.getEngine());

        tableDefinition.setIndices(indexTableDefinitions);
        DdlService ddlService = DdlService.root();
        ddlService.createTableWithInfo(schema.getSchemaName(), tableName, tableDefinition, connId, create.getOriginalCreateSql());

        RootCalciteSchema rootCalciteSchema = (RootCalciteSchema) context.getMutableRootSchema();
        RootSnapshotSchema rootSnapshotSchema = (RootSnapshotSchema) rootCalciteSchema.schema;
        SchemaDiff diff = SchemaDiff.builder().schemaId(schema.getSchemaId())
            .type(ActionType.ActionCreateTable).build();
        diff.setTableName(tableName);
        rootSnapshotSchema.applyDiff(diff);

        timeCtx.stop();
        long end = System.currentTimeMillis();
        long cost = end - start;
        DdlUtil.tableMap.put(tableName.toUpperCase(), schema.getSchemaName());
        if (cost > 10000) {
            LogUtils.info(log, "[ddl] create table take long time, "
                + "cost:{}, schemaName:{}, tableName:{}", cost, schema.getSchemaName(), tableName);
        } else {
            LogUtils.info(log, "[ddl] create table success, "
                + "cost:{}, schemaName:{}, tableName:{}", cost, schema.getSchemaName(), tableName);
        }
    }

    private static void validateEngine(List<IndexDefinition> indexTableDefinitions, String engine) {
        if (!indexTableDefinitions.isEmpty()) {
            if (isNotTxnEngine(engine)) {
                throw new IllegalArgumentException("Table with index, the engine must be transactional.");
            }
            indexTableDefinitions.stream()
                .filter(index -> isNotTxnEngine(index.getEngine()))
                .findAny().ifPresent(index -> {
                    throw new IllegalArgumentException("Index [" + index.getName() + "] engine must be transactional.");
                });
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
        if (schemaInfo == null) {
            throw DINGO_RESOURCE.unknownSchema(schema.getSchemaName()).ex();
        }
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

        RootCalciteSchema rootCalciteSchema = (RootCalciteSchema) context.getMutableRootSchema();
        RootSnapshotSchema rootSnapshotSchema = (RootSnapshotSchema) rootCalciteSchema.schema;
        SchemaDiff diff = SchemaDiff.builder().schemaId(schema.getSchemaId())
            .type(ActionType.ActionDropTable).tableId(table.tableId.seq).build();
        rootSnapshotSchema.applyDiff(diff);

        timeCtx.stop();
        long cost = System.currentTimeMillis() - start;
        DdlUtil.tableMap.remove(tableName.toUpperCase());
        if (cost > 10000) {
            LogUtils.info(log, "drop table take long time, cost:{}, schemaName:{}, tableName:{}", cost, schemaInfo.getName(), tableName);
        } else {
            LogUtils.info(log, "[ddl] drop table success, cost:{}, schemaName:{}, tableName:{}", cost, schema.getSchemaName(), tableName);
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

        RootCalciteSchema rootCalciteSchema = (RootCalciteSchema) context.getMutableRootSchema();
        RootSnapshotSchema rootSnapshotSchema = (RootSnapshotSchema) rootCalciteSchema.schema;
        SchemaDiff diff = SchemaDiff.builder().schemaId(schema.getSchemaId())
            .type(ActionType.ActionTruncateTable)
            .oldSchemaId(schema.getSchemaId())
            .oldTableId(table.tableId.seq)
            .build();
        diff.setTableName(tableName);
        rootSnapshotSchema.applyDiff(diff);

        timeCtx.stop();
        long cost = System.currentTimeMillis() - start;
        if (cost > 10000) {
            LogUtils.info(log, "truncate table cost long time, cost:{}, schemaName:{}, tableName:{}", cost, schemaInfo.getName(), tableName);
        } else {
            LogUtils.info(log, "truncate table success, cost:{}, schemaName:{}, tableName:{}", cost, schemaInfo.getName(), tableName);
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
        SchemaInfo schemaInfo = schema.getSchemaInfo(schema.getSchemaName());
        if (schemaInfo == null) {
            throw DINGO_RESOURCE.unknownSchema(schema.getSchemaName()).ex();
        }
        Table definition = schema.getTableInfo(tableName);
        if (definition == null) {
            throw DINGO_RESOURCE.tableNotExists(tableName).ex();
        } else {
            if (isNotTxnEngine(definition.getEngine())) {
                throw new IllegalArgumentException("Add column, the engine must be transactional.");
            }
        }
        DingoSqlColumn dingoSqlColumn = (DingoSqlColumn) sqlAlterAddColumn.getColumnDeclaration();
        ColumnDefinition newColumn = fromSqlColumnDeclaration(
            dingoSqlColumn,
            new ContextSqlValidator(context, true),
            definition.keyColumns().stream().map(Column::getName).collect(Collectors.toList())
        );
        if (newColumn == null) {
            throw new RuntimeException("newColumn is null.");
        }

        if (definition.getColumn(newColumn.getName()) != null) {
            throw new RuntimeException("Duplicate column name '" + newColumn.getName() + "'");
        }
        if (dingoSqlColumn.isPrimaryKey()) {
            throw DINGO_RESOURCE.addColumnPrimaryError(newColumn.getName(), tableName).ex();
        }
        if (dingoSqlColumn.isAutoIncrement()) {
            throw DINGO_RESOURCE.addColumnAutoIncError(newColumn.getName(), tableName).ex();
        }
        newColumn.setSchemaState(SchemaState.SCHEMA_NONE);
        validateAddColumn(newColumn);
        DdlService.root().addColumn(schemaInfo, definition, newColumn, "");

        RootCalciteSchema rootCalciteSchema = (RootCalciteSchema) context.getMutableRootSchema();
        RootSnapshotSchema rootSnapshotSchema = (RootSnapshotSchema) rootCalciteSchema.schema;
        SchemaDiff diff = SchemaDiff.builder().schemaId(schema.getSchemaId())
            .tableId(definition.getTableId().seq)
            .type(ActionType.ActionAddColumn)
            .build();
        rootSnapshotSchema.applyDiff(diff);

        LogUtils.info(log, "add column done, tableName:{}, colName:{}", tableName, newColumn.getName());
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
            throw DINGO_RESOURCE.tableNotExists(tableName).ex();
        }
        TableDefinition indexDef = fromSqlIndexDeclaration(
            sqlAlterAddIndex.getIndexDeclaration(), fromTable(table.getTable())
        );
        if (isNotTxnEngine(table.getTable().getEngine())) {
            throw new IllegalArgumentException("Table with index, the engine must be transactional.");
        }
        validateIndex(schema, tableName, indexDef);
        DdlService ddlService = DdlService.root();
        ddlService.createIndex(schema.getSchemaName() , tableName, indexDef);

        RootCalciteSchema rootCalciteSchema = (RootCalciteSchema) context.getMutableRootSchema();
        RootSnapshotSchema rootSnapshotSchema = (RootSnapshotSchema) rootCalciteSchema.schema;
        SchemaDiff diff = SchemaDiff.builder().schemaId(schema.getSchemaId())
            .tableId(table.getTableId().seq)
            .type(ActionType.ActionAddIndex)
            .build();
        rootSnapshotSchema.applyDiff(diff);
        LogUtils.info(log, "create index done, tableName:{}, index:{}", tableName, indexDef.getName());
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
            throw DINGO_RESOURCE.tableNotExists(tableName).ex();
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
        if (isNotTxnEngine(table.getTable().getEngine())) {
            throw new IllegalArgumentException("Table with index, the engine must be transactional.");
        }
        IndexDefinition indexDef = fromSqlIndexDeclaration(sqlIndexDeclaration, fromTable(table.getTable()));
        indexDef.setUnique(sqlCreateIndex.isUnique);
        validateIndex(schema, tableName, indexDef);
        DdlService.root().createIndex(schema.getSchemaName(), tableName, indexDef);

        RootCalciteSchema rootCalciteSchema = (RootCalciteSchema) context.getMutableRootSchema();
        RootSnapshotSchema rootSnapshotSchema = (RootSnapshotSchema) rootCalciteSchema.schema;
        SchemaDiff diff = SchemaDiff.builder().schemaId(schema.getSchemaId())
            .tableId(table.getTableId().seq)
            .type(ActionType.ActionAddIndex)
            .build();
        rootSnapshotSchema.applyDiff(diff);
    }

    public void execute(@NonNull SqlDropIndex sqlDropIndex, CalcitePrepare.Context context) {
        final Pair<SubSnapshotSchema, String> schemaTableName
            = getSchemaAndTableName(sqlDropIndex.table, context);
        final String tableName = Parameters.nonNull(schemaTableName.right, "table name");
        final SubSnapshotSchema schema = Parameters.nonNull(schemaTableName.left, "table schema");
        DingoTable table = schema.getTable(tableName);
        if (table == null) {
            throw DINGO_RESOURCE.tableNotExists(tableName).ex();
        }
        validateDropIndex(table, sqlDropIndex.index);
        DdlService.root().dropIndex(schema.getSchemaName(), tableName, sqlDropIndex.index);

        RootCalciteSchema rootCalciteSchema = (RootCalciteSchema) context.getMutableRootSchema();
        RootSnapshotSchema rootSnapshotSchema = (RootSnapshotSchema) rootCalciteSchema.schema;
        SchemaDiff diff = SchemaDiff.builder().schemaId(schema.getSchemaId())
            .tableId(table.getTableId().seq)
            .type(ActionType.ActionDropIndex)
            .build();
        rootSnapshotSchema.applyDiff(diff);
        LogUtils.info(log, "drop index done, tableName:{}, index:{}", tableName, sqlDropIndex.index);
    }

    public void execute(@NonNull SqlAlterDropIndex sqlDropIndex, CalcitePrepare.Context context) {
        final Pair<SubSnapshotSchema, String> schemaTableName
            = getSchemaAndTableName(sqlDropIndex.table, context);
        final String tableName = Parameters.nonNull(schemaTableName.right, "table name");
        final SubSnapshotSchema schema = Parameters.nonNull(schemaTableName.left, "table schema");
        DingoTable table = schema.getTable(tableName);
        if (table == null) {
            throw DINGO_RESOURCE.tableNotExists(tableName).ex();
        }
        validateDropIndex(table, sqlDropIndex.getIndexNm());
        DdlService.root().dropIndex(schema.getSchemaName(), tableName, sqlDropIndex.getIndexNm());

        RootCalciteSchema rootCalciteSchema = (RootCalciteSchema) context.getMutableRootSchema();
        RootSnapshotSchema rootSnapshotSchema = (RootSnapshotSchema) rootCalciteSchema.schema;
        SchemaDiff diff = SchemaDiff.builder().schemaId(schema.getSchemaId())
            .tableId(table.getTableId().seq)
            .type(ActionType.ActionDropIndex)
            .build();
        rootSnapshotSchema.applyDiff(diff);
        LogUtils.info(log, "drop index done, tableName:{}, index:{}", tableName, sqlDropIndex.getIndexNm());
    }

    public void execute(SqlAlterDropColumn sqlAlterDropColumn, CalcitePrepare.Context context) {
        final Pair<SubSnapshotSchema, String> schemaTableName
            = getSchemaAndTableName(sqlAlterDropColumn.table, context);
        final String tableName = Parameters.nonNull(schemaTableName.right, "table name");
        final SubSnapshotSchema schema = Parameters.nonNull(schemaTableName.left, "table schema");
        Table table = schema.getTableInfo(tableName);
        if (table == null) {
            throw DINGO_RESOURCE.tableNotExists(tableName).ex();
        } else {
            if (isNotTxnEngine(table.getEngine())) {
                throw new IllegalArgumentException("Drop index, the engine must be transactional.");
            }
        }
        String dropColumn = sqlAlterDropColumn.columnNm.toUpperCase();
        boolean noneMatchCol = table.getColumns().stream()
            .noneMatch(column -> column.getSchemaState() == SchemaState.SCHEMA_PUBLIC
                && column.getName().equalsIgnoreCase(dropColumn));
        if (noneMatchCol) {
            throw new RuntimeException("column " + dropColumn +  " does not exists");
        }

        if (Objects.requireNonNull(table.getColumn(dropColumn)).isPrimary()) {
            throw DINGO_RESOURCE.dropColumnError().ex();
        }
        // can't drop column age with composite index covered or Primary Key covered now
        List<IndexTable> matchIndices = table.getIndexes().stream().filter(indexTable -> {
            boolean keyContains = indexTable.getOriginKeyList().contains(dropColumn);
            boolean withKeyContains = false;
            if (indexTable.getOriginWithKeyList() != null) {
                withKeyContains = indexTable.getOriginWithKeyList().contains(dropColumn);
            }
            return keyContains || withKeyContains;
        }).collect(Collectors.toList());

        // If the index column contains the column to be deleted and the primary key only has this column,
        // it is necessary to mark the deletion
        List<String> indicesInfo = matchIndices
            .stream().filter(indexTable -> {
                boolean keyContains = indexTable.getOriginKeyList().contains(dropColumn);
                return keyContains && indexTable.getOriginKeyList().size() == 1
                    && (indexTable.getOriginWithKeyList() == null || indexTable.getOriginWithKeyList().isEmpty());
            }).map(Table::getName).collect(Collectors.toList());
        if (matchIndices.size() > indicesInfo.size()) {
            throw DINGO_RESOURCE.dropColumnError().ex();
        }

        String markedDelete = "";
        if (!indicesInfo.isEmpty()) {
            markedDelete = String.join(",", indicesInfo);
        }

        DdlService.root().dropColumn(
            table.getTableId().domain,
            schema.getSchemaName(),
            table.tableId.seq,
            tableName,
            sqlAlterDropColumn.columnNm,
             markedDelete,
             "",
             "");

        RootCalciteSchema rootCalciteSchema = (RootCalciteSchema) context.getMutableRootSchema();
        RootSnapshotSchema rootSnapshotSchema = (RootSnapshotSchema) rootCalciteSchema.schema;
        SchemaDiff diff = SchemaDiff.builder().schemaId(schema.getSchemaId())
            .tableId(table.getTableId().seq)
            .type(ActionType.ActionDropColumn)
            .build();
        rootSnapshotSchema.applyDiff(diff);
        LogUtils.info(log, "drop column done, tableName:{}, column:{}", tableName, sqlAlterDropColumn.columnNm);
    }

    public static void validateDropIndex(DingoTable table, String indexName) {
        if (isNotTxnEngine(table.getTable().getEngine())) {
            throw new IllegalArgumentException("Drop index, the engine must be transactional.");
        }
        if (table.getIndexTableDefinitions().stream().map(IndexTable::getName).noneMatch(indexName::equalsIgnoreCase)) {
            throw new RuntimeException("The index " + indexName + " not exist.");
        }
    }

    public static void validateIndex(SubSnapshotSchema schema, String tableName, TableDefinition index) {
        if (isNotTxnEngine(index.getEngine())) {
            throw new IllegalArgumentException("the index engine must be transactional.");
        }
        DingoTable table = schema.getTable(tableName);
        if (table == null) {
            throw DINGO_RESOURCE.tableNotExists(tableName).ex();
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
            CalciteSchema schema = context.getRootSchema().getSubSchema(schemaName, false);
            if (schema == null) {
                throw new RuntimeException("schema " + schemaName + " does not exist");
            }
            InfoSchema is = null;
            if (schema instanceof SubCalciteSchema) {
                SubCalciteSchema sub = (SubCalciteSchema) schema;
                SubSnapshotSchema subSnapshotSchema = (SubSnapshotSchema) sub.schema;
                is = subSnapshotSchema.getIs();
            }
            if (is == null) {
                is = DdlService.root().getIsLatest();
                if (is == null) {
                    LogUtils.error(log, "getPrivilegeDefinition get is null");
                    throw new RuntimeException("table " + tableName + " does not exist");
                }
            }
            if (is.getTable(schemaName, tableName) == null) {
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

    public static int getReplica(int targetReplica, int type) {
        int replica;
        if (type == 1) {
            replica = InfoSchemaService.root().getStoreReplica();
            if (targetReplica == 0) {
                return replica;
            }
            if (targetReplica > 0 && replica < targetReplica) {
                throw DINGO_RESOURCE.notEnoughRegion().ex();
            }
        } else if (type == 2) {
            replica = InfoSchemaService.root().getIndexReplica();
            if (targetReplica == 0) {
                return replica;
            }
            if (targetReplica > 0 && replica < targetReplica) {
                throw DINGO_RESOURCE.notEnoughRegion().ex();
            }
        } else if (type == 3) {
            replica = InfoSchemaService.root().getDocumentReplica();
            if (targetReplica == 0) {
                return replica;
            }
            if (targetReplica > 0 && replica < targetReplica) {
                throw DINGO_RESOURCE.notEnoughRegion().ex();
            }
        }
        return targetReplica;
    }

    private static void validateAddColumn(ColumnDefinition newColumn) {
        DingoType type = newColumn.getType();
        if (newColumn.getDefaultValue() == null) {
            if (!newColumn.isNullable()) {
                if (type instanceof StringType) {
                    newColumn.setDefaultValue("");
                } else if (type instanceof LongType
                    || type instanceof IntegerType || type instanceof DoubleType
                    || type instanceof FloatType || type instanceof DecimalType) {
                    newColumn.setDefaultValue("0");
                } else if (type instanceof DateType) {
                    newColumn.setDefaultValue("0000-00-00");
                } else if (type instanceof BooleanType) {
                    newColumn.setDefaultValue("false");
                } else if (type instanceof TimestampType) {
                    newColumn.setDefaultValue("0000-00-00 00:00:00");
                } else if (type instanceof TimeType) {
                    newColumn.setDefaultValue("00:00:00");
                } else if (type instanceof ListType || type instanceof MapType) {
                    newColumn.setDefaultValue("{}");
                }
            }
        } else {
            try {
                String defaultVal = newColumn.getDefaultValue();
                if (type instanceof LongType) {
                    Long.parseLong(defaultVal);
                } else if (type instanceof IntegerType) {
                    Integer.parseInt(defaultVal);
                } else if (type instanceof DoubleType) {
                    Double.parseDouble(defaultVal);
                } else if (type instanceof FloatType) {
                    Float.parseFloat(defaultVal);
                } else if (type instanceof DateType) {
                    if ("current_date".equalsIgnoreCase(defaultVal)) {
                        return;
                    }
                    DateTimeUtils.parseDate(defaultVal);
                } else if (type instanceof DecimalType) {
                    new BigDecimal(defaultVal);
                } else if (type instanceof BooleanType) {
                    boolean res = defaultVal.equalsIgnoreCase("true")
                        || defaultVal.equalsIgnoreCase("false") || defaultVal.equalsIgnoreCase("1")
                        || defaultVal.equalsIgnoreCase("0");
                    if (!res) {
                        throw new RuntimeException("Invalid default value");
                    }
                } else if (type instanceof TimestampType) {
                    if (defaultVal.equalsIgnoreCase("current_timestamp")) {
                        return;
                    }
                    DateTimeUtils.parseTimestamp(defaultVal);
                } else if (type instanceof TimeType) {
                    DateTimeUtils.parseTime(defaultVal);
                } else if (type instanceof ListType) {
                    if (defaultVal.toUpperCase().startsWith("ARRAY[") && defaultVal.endsWith("]")) {
                        defaultVal = defaultVal.substring(6, defaultVal.length() - 1);
                        List<String> listVal = Arrays.asList(defaultVal.split(","));
                        listVal.forEach(item -> {
                            switch (newColumn.getElementType()) {
                                case "FLOAT":
                                    Float.parseFloat(item);
                                    break;
                                case "DOUBLE":
                                    Double.parseDouble(item);
                                    break;
                                case "INTEGER":
                                    Integer.parseInt(item);
                                    break;
                                case "LONG":
                                    Long.parseLong(item);
                                    break;
                                case "BOOLEAN":
                                    Boolean.parseBoolean(item);
                                    break;
                                case "DATE":
                                    DateTimeUtils.parseDate(item);
                                    break;
                                case "DECIMAL":
                                    new BigDecimal(item);
                                    break;
                                case "TIMESTAMP":
                                    DateTimeUtils.parseTimestamp(item);
                                    break;
                                case "TIME":
                                    DateTimeUtils.parseTime(item);
                                    break;
                                default:
                                    break;
                            }
                        });
                        newColumn.setDefaultValue(defaultVal);
                    } else {
                        throw DINGO_RESOURCE.invalidDefaultValue(newColumn.getDefaultValue()).ex();
                    }
                } else if (type instanceof MapType) {
                    if (defaultVal.toUpperCase().startsWith("MAP[") && defaultVal.endsWith("]")) {
                        defaultVal = defaultVal.substring(4, defaultVal.length() - 1);
                        int itemSize = defaultVal.split(",").length;
                        if (itemSize % 2 != 0) {
                            throw DINGO_RESOURCE.invalidDefaultValue(newColumn.getDefaultValue()).ex();
                        }
                        newColumn.setDefaultValue(defaultVal);
                    }
                }
            } catch (Exception e) {
                throw DINGO_RESOURCE.invalidDefaultValue(newColumn.getDefaultValue()).ex();
            }
        }

    }

}
