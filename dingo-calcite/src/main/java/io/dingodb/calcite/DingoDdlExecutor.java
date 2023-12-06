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

import io.dingodb.calcite.grammar.ddl.DingoSqlCreateTable;
import io.dingodb.calcite.grammar.ddl.SqlAlterAddIndex;
import io.dingodb.calcite.grammar.ddl.SqlAlterConvertCharset;
import io.dingodb.calcite.grammar.ddl.SqlAlterTableDistribution;
import io.dingodb.calcite.grammar.ddl.SqlAlterUser;
import io.dingodb.calcite.grammar.ddl.SqlCreateIndex;
import io.dingodb.calcite.grammar.ddl.SqlCreateSchema;
import io.dingodb.calcite.grammar.ddl.SqlCreateUser;
import io.dingodb.calcite.grammar.ddl.SqlDropIndex;
import io.dingodb.calcite.grammar.ddl.SqlDropUser;
import io.dingodb.calcite.grammar.ddl.SqlFlushPrivileges;
import io.dingodb.calcite.grammar.ddl.SqlGrant;
import io.dingodb.calcite.grammar.ddl.SqlIndexDeclaration;
import io.dingodb.calcite.grammar.ddl.SqlRevoke;
import io.dingodb.calcite.grammar.ddl.SqlSetPassword;
import io.dingodb.calcite.grammar.ddl.SqlTruncate;
import io.dingodb.calcite.schema.DingoRootSchema;
import io.dingodb.calcite.schema.DingoSchema;
import io.dingodb.common.CommonId;
import io.dingodb.common.environment.ExecutionEnvironment;
import io.dingodb.common.mysql.SchemataDefinition;
import io.dingodb.common.partition.PartitionDefinition;
import io.dingodb.common.partition.PartitionDetailDefinition;
import io.dingodb.common.privilege.PrivilegeDefinition;
import io.dingodb.common.privilege.PrivilegeType;
import io.dingodb.common.privilege.SchemaPrivDefinition;
import io.dingodb.common.privilege.TablePrivDefinition;
import io.dingodb.common.privilege.UserDefinition;
import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.common.table.Index;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.DefinitionUtils;
import io.dingodb.common.util.Optional;
import io.dingodb.common.util.Parameters;
import io.dingodb.partition.DingoPartitionServiceProvider;
import io.dingodb.verify.plugin.AlgorithmPlugin;
import io.dingodb.verify.service.InformationService;
import io.dingodb.verify.service.InformationServiceProvider;
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
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.ddl.SqlDropSchema;
import org.apache.calcite.sql.ddl.SqlDropTable;
import org.apache.calcite.sql.ddl.SqlKeyConstraint;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static io.dingodb.calcite.runtime.DingoResource.DINGO_RESOURCE;
import static io.dingodb.common.util.Optional.mapOrNull;
import static io.dingodb.common.util.PrivilegeUtils.getRealAddress;
import static io.dingodb.common.util.Utils.calculatePrefixCount;
import static org.apache.calcite.util.Static.RESOURCE;

@Slf4j
public class DingoDdlExecutor extends DdlExecutorImpl {
    public static final DingoDdlExecutor INSTANCE = new DingoDdlExecutor();

    private static final Pattern namePattern = Pattern.compile("^[A-Z_][A-Z\\d_]+$");

    private final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    public UserService userService;
    public InformationService informationService;

    private DingoDdlExecutor() {
        this.userService = UserServiceProvider.getRoot();
        this.informationService = InformationServiceProvider.getRoot();
    }

    private List<TableDefinition> getIndexDefinitions(DingoSqlCreateTable create, TableDefinition tableDefinition) {
        assert create.columnList != null;
        return create.columnList.stream()
            .filter(col -> col.getKind() == SqlKind.CREATE_INDEX)
            .map(col -> fromSqlIndexDeclaration((SqlIndexDeclaration) col, tableDefinition))
            .collect(Collectors.toCollection(ArrayList::new));
    }

    private @Nullable TableDefinition fromSqlIndexDeclaration(
        @NonNull SqlIndexDeclaration indexDeclaration,
        TableDefinition tableDefinition
    ) {
        List<ColumnDefinition> tableColumns = tableDefinition.getColumns();
        List<String> tableColumnNames = tableColumns.stream().map(ColumnDefinition::getName)
            .collect(Collectors.toList());

        // Primary key list
        List<String> columns = indexDeclaration.columnList.getList().stream()
            .filter(Objects::nonNull)
            .map(SqlIdentifier.class::cast)
            .map(SqlIdentifier::getSimple)
            .map(String::toUpperCase)
            .collect(Collectors.toCollection(ArrayList::new));

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
        if (indexDeclaration.getIndexType().equalsIgnoreCase("scalar")) {
            properties.put("indexType", "scalar");
            for (int i = 0; i < columns.size(); i++) {
                String columnName = columns.get(i);
                if (!tableColumnNames.contains(columnName)) {
                    throw new RuntimeException("Invalid column name: " + columnName);
                }

                ColumnDefinition columnDefinition = tableColumns.stream().filter(f -> f.getName().equals(columnName))
                    .findFirst().get();

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
        } else {
            properties.put("indexType", "vector");
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
                } else if (i == 1) {
                    if (!columnDefinition.getTypeName().equals("ARRAY")
                        || !(columnDefinition.getElementType() != null
                            && columnDefinition.getElementType().equals("FLOAT"))) {
                        throw new RuntimeException("Invalid column type: " + columnName);
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
            String[] otherColumns = indexDeclaration.withColumnList.getList().stream()
                .filter(Objects::nonNull)
                .map(SqlIdentifier.class::cast)
                .map(SqlIdentifier::getSimple)
                .map(String::toUpperCase)
                .toArray(String[]::new);

            for (String columnName : otherColumns) {
                // Check if the column exists in the original table
                if (!tableColumnNames.contains(columnName)) {
                    throw new RuntimeException("Invalid column name: " + columnName);
                }

                if (columns.contains(columnName)) {
                    continue;
                }

                ColumnDefinition columnDefinition = tableColumns.stream().filter(f -> f.getName().equals(columnName))
                    .findFirst().get();

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
        indexTableDefinition.setReplica(indexDeclaration.getReplica());
        indexTableDefinition.setProperties(properties);

        Optional.ifPresent(indexDeclaration.getPartDefinition(),
            __ -> validatePartitionBy(indexTableDefinition.getKeyColumns().stream()
                .map(ColumnDefinition::getName).collect(Collectors.toList()), indexTableDefinition, __));

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
        if (scd.isAutoIncrement()) {
            if (SqlTypeName.INT_TYPES.contains(typeName)) {
                defaultValue = "";
            } else {
                throw DINGO_RESOURCE.specifierForColumn(name).ex();
            }
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

    private static @NonNull Pair<DingoSchema, String> getSchemaAndTableName(
        @NonNull SqlIdentifier id, CalcitePrepare.@NonNull Context context
    ) {
        CalciteSchema rootSchema = context.getMutableRootSchema();
        assert rootSchema != null : "No root schema.";
        final List<String> defaultSchemaPath = context.getDefaultSchemaPath();
        assert defaultSchemaPath.size() == 1 : "Assume that the schema path has only one level.";
        // todo: current version, ignore name case
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
            // todo: current version, ignore name case
            CalciteSchema subSchema = rootSchema.getSubSchema(names.get(0), false);
            if (subSchema != null) {
                schema = subSchema.schema;
                tableName = String.join(".", Util.skip(names));
            } else {
                schema = defaultSchema.schema;
                tableName = String.join(".", names);
            }
        }

        if (!(schema instanceof DingoSchema)) {
            throw new AssertionError("The schema not found or not belong to dingo.");
        }
        return Pair.of((DingoSchema) schema, tableName.toUpperCase());
    }

    public void execute(SqlCreateSchema schema, CalcitePrepare.Context context) {
        DingoRootSchema rootSchema = (DingoRootSchema) context.getMutableRootSchema().schema;
        String schemaName = schema.name.names.get(0);
        rootSchema.createSubSchema(schemaName);
        // character default utf8, collate default val:utf8_bin
        informationService.saveSchemata(
            new SchemataDefinition("def", schemaName, schema.charset, schema.collate, null)
        );
    }

    public void execute(SqlDropSchema schema, CalcitePrepare.Context context) {
        DingoRootSchema rootSchema = (DingoRootSchema) context.getMutableRootSchema().schema;
        String schemaName = schema.name.names.get(0);
        if (schemaName.equalsIgnoreCase(context.getDefaultSchemaPath().get(0))) {
            throw new RuntimeException("Schema used.");
        }
        Schema subSchema = rootSchema.getSubSchema(schemaName);
        if (subSchema == null) {
            throw DINGO_RESOURCE.unknownSchema(schemaName).ex();
        }
        if (subSchema.getTableNames().isEmpty()) {
            rootSchema.dropSubSchema(schemaName);
            informationService.dropSchemata(schemaName);
        } else {
            throw new RuntimeException("Schema not empty.");
        }
    }

    @SuppressWarnings({"unused"})
    public void execute(SqlCreateTable createT, CalcitePrepare.Context context) {
        DingoSqlCreateTable create = (DingoSqlCreateTable) createT;
        log.info("DDL execute: {}", create);
        final Pair<DingoSchema, String> schemaTableName
            = getSchemaAndTableName(create.name, context);
        SqlNodeList columnList = create.columnList;
        if (columnList == null) {
            throw SqlUtil.newContextException(create.name.getParserPosition(),
                RESOURCE.createTableRequiresColumnList());
        }
        final String tableName = Parameters.nonNull(schemaTableName.right, "table name");

        // Get all primary key
        List<String> pks = create.columnList.stream()
            .filter(SqlKeyConstraint.class::isInstance)
            .map(SqlKeyConstraint.class::cast)
            .filter(constraint -> constraint.getOperator().getKind() == SqlKind.PRIMARY_KEY)
            // The 0th element is the name of the constraint
            .map(constraint -> (SqlNodeList) constraint.getOperandList().get(1))
            .findAny().map(sqlNodes -> sqlNodes.getList().stream()
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

        SqlValidator validator = new ContextSqlValidator(context, true);

        // Mapping, column node -> column definition
        List<ColumnDefinition> columns = create.columnList.stream()
            .filter(col -> col.getKind() == SqlKind.COLUMN_DECL)
            .map(col -> fromSqlColumnDeclaration((DingoSqlColumn) col, validator, pks))
            .collect(Collectors.toCollection(ArrayList::new));
        // If it is a table without a primary key, create an invisible column _rowid is a self increasing primary key
        if (pks.size() == 0) {
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
            throw new RuntimeException("Duplicate column names are not allowed in table definition. Total: "
                + realColCnt + ", distinct: " + distinctColCnt);
        }

        final DingoSchema schema = Parameters.nonNull(schemaTableName.left, "table schema");

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

        TableDefinition tableDefinition = new TableDefinition(
            tableName,
            columns,
            new ConcurrentHashMap<>(),
            1,
            create.getTtl(),
            create.getPartDefinition(),
            create.getEngine(),
            create.getProperties(),
            create.getAutoIncrement(),
            create.getReplica(),
            create.getOriginalCreateSql(),
            create.getComment(),
            create.getCharset(),
            create.getCollate()
        );
        List<TableDefinition> indexTableDefinitions = getIndexDefinitions(create, tableDefinition);

        // Validate partition strategy
        Optional.ifPresent(create.getPartDefinition(), __ -> validatePartitionBy(pks, tableDefinition, __));

        schema.createTables(tableDefinition, indexTableDefinitions);
        informationService.saveTableMeta(schema.name(), tableDefinition);
    }

    @SuppressWarnings({"unused", "MethodMayBeStatic"})
    public void execute(SqlDropTable drop, CalcitePrepare.Context context) {
        log.info("DDL execute: {}", drop);
        final Pair<DingoSchema, String> schemaTableName
            = getSchemaAndTableName(drop.name, context);
        final DingoSchema schema = schemaTableName.left;
        final String tableName = schemaTableName.right;
        final boolean existed;
        assert schema != null;
        assert tableName != null;
        existed = schema.dropTable(tableName);
        if (!existed && !drop.ifExists) {
            String schemaName = schema.getMetaService().name();
            throw DINGO_RESOURCE.unknownTable(schemaName + "." + tableName).ex();
        }

        env.getTableIdMap().computeIfPresent(schema.id(), (k, v) -> {
            v.remove(tableName);
            return v;
        });
        userService.dropTablePrivilege(schema.name(), tableName);
        informationService.dropTableMeta(schema.name(), tableName);
    }

    public void execute(@NonNull SqlTruncate truncate, CalcitePrepare.Context context) {
        SqlIdentifier name = (SqlIdentifier) truncate.getOperandList().get(0);
        final Pair<DingoSchema, String> schemaTableName
            = getSchemaAndTableName(name, context);
        final DingoSchema schema = Parameters.nonNull(schemaTableName.left, "table schema");
        String tableName = Parameters.nonNull(schemaTableName.right, "table name").toUpperCase();
        TableDefinition tableDefinition = schema.getMetaService().getTableDefinition(tableName);
        Map<CommonId, TableDefinition> indexDefinitionMap = schema.getMetaService().getTableIndexDefinitions(tableName);
        if (tableDefinition == null) {
            throw SqlUtil.newContextException(
                name.getParserPosition(),
                RESOURCE.tableNotFound(name.toString()));
        }
        if (!schema.dropTable(tableName)) {
            throw SqlUtil.newContextException(
                name.getParserPosition(),
                RESOURCE.tableNotFound(name.toString())
            );
        }
        // partitionDetail operand [x,null,null] error  transform to operand[x]
        List<TableDefinition> indexTableDefinitions = indexDefinitionMap.values()
            .stream().map(e -> {
                if (e.getPartDefinition() == null) {
                    return e;
                }
                e.getPartDefinition().getDetails().forEach(DingoDdlExecutor::transformOperand);
                return e;
            }).collect(Collectors.toList());
        if (tableDefinition.getPartDefinition() != null) {
            tableDefinition.getPartDefinition().getDetails().forEach(DingoDdlExecutor::transformOperand);
        }
        schema.createTables(tableDefinition, indexTableDefinitions);
    }

    private static void transformOperand(PartitionDetailDefinition detail) {
        int i = calculatePrefixCount(detail.getOperand());
        Object[] val = new Object[i];
        System.arraycopy(detail.getOperand(), 0, val, 0, i);
        detail.setOperand(val);
    }


    public void execute(@NonNull SqlGrant sqlGrant, CalcitePrepare.Context context) {
        log.info("DDL execute: {}", sqlGrant);
        if (!"*".equals(sqlGrant.table)) {
            SqlIdentifier name = sqlGrant.tableIdentifier;
            final Pair<DingoSchema, String> schemaTableName
                = getSchemaAndTableName(name, context);
            final String tableName = Parameters.nonNull(schemaTableName.right, "table name");
            final DingoSchema schema = Parameters.nonNull(schemaTableName.left, "table schema");
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

    public void execute(@NonNull SqlRevoke sqlRevoke, CalcitePrepare.Context context) {
        log.info("DDL execute: {}", sqlRevoke);
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
        log.info("DDL execute: {}", sqlCreateUser);
        UserDefinition userDefinition = UserDefinition.builder().user(sqlCreateUser.user)
            .host(getRealAddress(sqlCreateUser.host)).build();
        if (userService.existsUser(userDefinition)) {
            throw DINGO_RESOURCE.createUserFailed(sqlCreateUser.user, sqlCreateUser.host).ex();
        } else {
            userDefinition.setPlugin("mysql_native_password");
            userDefinition.setRequireSsl(sqlCreateUser.requireSsl);
            userDefinition.setLock(sqlCreateUser.lock);
            String digestPwd = AlgorithmPlugin.digestAlgorithm(sqlCreateUser.password, userDefinition.getPlugin());
            userDefinition.setPassword(digestPwd);
            userDefinition.setExpireDays(sqlCreateUser.expireDays);
            userService.createUser(userDefinition);
        }
    }

    public void execute(@NonNull SqlDropUser sqlDropUser, CalcitePrepare.Context context) {
        log.info("DDL execute: {}", sqlDropUser);
        UserDefinition userDefinition = UserDefinition.builder().user(sqlDropUser.name)
            .host(getRealAddress(sqlDropUser.host)).build();
        if (!userService.existsUser(userDefinition)) {
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
        log.info("DDL execute: {}", sqlAlterTableDistribution);
        final Pair<DingoSchema, String> schemaTableName
            = getSchemaAndTableName(sqlAlterTableDistribution.table, context);
        final String tableName = Parameters.nonNull(schemaTableName.right, "table name");
        DingoSchema schema = Parameters.nonNull(schemaTableName.left, "table schema");
        TableDefinition tableDefinition = schema.getMetaService().getTableDefinition(tableName);
        PartitionDetailDefinition detail = sqlAlterTableDistribution.getPartitionDefinition();
        DefinitionUtils.checkAndConvertRangePartitionDetail(tableDefinition, detail);
        schema.addDistribution(tableName, sqlAlterTableDistribution.getPartitionDefinition());
    }

    public void execute(@NonNull SqlAlterAddIndex sqlAlterAddIndex, CalcitePrepare.Context context) {
        final Pair<DingoSchema, String> schemaTableName
            = getSchemaAndTableName(sqlAlterAddIndex.table, context);
        final String tableName = Parameters.nonNull(schemaTableName.right, "table name");
        final DingoSchema schema = Parameters.nonNull(schemaTableName.left, "table schema");
        Index index = new Index(sqlAlterAddIndex.index, sqlAlterAddIndex.getColumnNames(), sqlAlterAddIndex.isUnique);
        validateIndex(schema, tableName, index);
        schema.createIndex(tableName, Collections.singletonList(index));
    }

    public void execute(@NonNull SqlCreateIndex sqlCreateIndex, CalcitePrepare.Context context) {
        final Pair<DingoSchema, String> schemaTableName
            = getSchemaAndTableName(sqlCreateIndex.table, context);
        final String tableName = Parameters.nonNull(schemaTableName.right, "table name");
        final DingoSchema schema = Parameters.nonNull(schemaTableName.left, "table schema");
        Index index = new Index(sqlCreateIndex.index, sqlCreateIndex.getColumnNames(), sqlCreateIndex.isUnique);
        validateIndex(schema, tableName, index);
        schema.createIndex(tableName, Arrays.asList(index));
    }

    public void execute(@NonNull SqlDropIndex sqlDropIndex, CalcitePrepare.Context context) {
        final Pair<DingoSchema, String> schemaTableName
            = getSchemaAndTableName(sqlDropIndex.table, context);
        final String tableName = Parameters.nonNull(schemaTableName.right, "table name");
        final DingoSchema schema = Parameters.nonNull(schemaTableName.left, "table schema");
        validateDropIndex(schema, tableName, sqlDropIndex.index);
        schema.dropIndex(tableName, sqlDropIndex.index);
    }

    public void execute(@NonNull SqlAlterUser sqlAlterUser, CalcitePrepare.Context context) {
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
    }

    public void execute(SqlAlterConvertCharset sqlAlterConvert, CalcitePrepare.Context context) {
        final Pair<DingoSchema, String> schemaTableName
            = getSchemaAndTableName(sqlAlterConvert.table, context);
        final String tableName = Parameters.nonNull(schemaTableName.right, "table name");
        final DingoSchema schema = Parameters.nonNull(schemaTableName.left, "table schema");
        TableDefinition tableDefinition = new TableDefinition(tableName, null, null,
            0, 0, null, "ENG_ROCKSDB", null, 1, 3,
            null, null, sqlAlterConvert.getCharset(), sqlAlterConvert.getCollate());
        informationService.updateTableMeta(schema.name(), tableDefinition);
    }

    public void validateDropIndex(DingoSchema schema, String tableName, String indexName) {
        if (schema.getTable(tableName) == null) {
            throw new IllegalArgumentException("table " + tableName + " does not exist ");
        }
        TableDefinition tableDefinition = schema.getMetaService().getTableDefinition(tableName);
        if (tableDefinition != null) {
            if (tableDefinition.getIndexes() == null) {
                throw new IllegalArgumentException("index " + indexName + " does not exist ");
            } else {
                if (!tableDefinition.getIndexes().containsKey(indexName)) {
                    throw new IllegalArgumentException("index " + indexName + " does not exist ");
                }
            }

        }
    }

    public void validateIndex(DingoSchema schema, String tableName, Index newIndex) {
        if (schema.getTable(tableName) == null) {
            throw new IllegalArgumentException("table " + tableName + " does not exist ");
        }
        TableDefinition tableDefinition = schema.getMetaService().getTableDefinition(tableName);
        if (tableDefinition != null) {
            tableDefinition.validationIndex(newIndex);
        }
    }

    public void validatePartitionBy(
        @NonNull List<String> keyList,
        @NonNull TableDefinition tableDefinition,
        @NonNull PartitionDefinition partDefinition
    ) {
        String strategy = partDefinition.getFuncName().toUpperCase();
        switch (strategy) {
            case DingoPartitionServiceProvider.RANGE_FUNC_NAME:
                DefinitionUtils.checkAndConvertRangePartition(tableDefinition);
                break;
            case DingoPartitionServiceProvider.HASH_FUNC_NAME:
                DefinitionUtils.checkAndConvertHashRangePartition(tableDefinition);
                break;
            default:
                throw new IllegalStateException("Unsupported " + strategy);
        }
    }

    @NonNull
    private static PrivilegeDefinition getPrivilegeDefinition(
        @NonNull SqlGrant sqlGrant, CalcitePrepare.@NonNull Context context
    ) {
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

}
