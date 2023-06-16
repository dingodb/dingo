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
import io.dingodb.calcite.grammar.ddl.SqlAlterTableDistribution;
import io.dingodb.calcite.grammar.ddl.SqlAlterUser;
import io.dingodb.calcite.grammar.ddl.SqlCommit;
import io.dingodb.calcite.grammar.ddl.SqlCreateIndex;
import io.dingodb.calcite.grammar.ddl.SqlCreateUser;
import io.dingodb.calcite.grammar.ddl.SqlDropIndex;
import io.dingodb.calcite.grammar.ddl.SqlDropUser;
import io.dingodb.calcite.grammar.ddl.SqlFlushPrivileges;
import io.dingodb.calcite.grammar.ddl.SqlGrant;
import io.dingodb.calcite.grammar.ddl.SqlIndexDeclaration;
import io.dingodb.calcite.grammar.ddl.SqlRevoke;
import io.dingodb.calcite.grammar.ddl.SqlRollback;
import io.dingodb.calcite.grammar.ddl.SqlSetPassword;
import io.dingodb.calcite.grammar.ddl.SqlTruncate;
import io.dingodb.calcite.grammar.ddl.SqlUseSchema;
import io.dingodb.common.environment.ExecutionEnvironment;
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
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSetOption;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.ddl.DingoSqlColumn;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.ddl.SqlDropTable;
import org.apache.calcite.sql.ddl.SqlKeyConstraint;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static io.dingodb.calcite.runtime.DingoResource.DINGO_RESOURCE;
import static io.dingodb.common.util.Optional.mapOrNull;
import static io.dingodb.common.util.PrivilegeUtils.getRealAddress;
import static org.apache.calcite.util.Static.RESOURCE;

@Slf4j
public class DingoDdlExecutor extends DdlExecutorImpl {
    public static final DingoDdlExecutor INSTANCE = new DingoDdlExecutor();

    private ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    public UserService userService;

    private DingoDdlExecutor() {
        this.userService = UserServiceProvider.getRoot();
    }

    private static List<Index> getIndex(DingoSqlCreateTable create) {
        List<Index> indexList = create.columnList.stream()
            .filter(col -> col.getKind() == SqlKind.CREATE_INDEX)
            .map(col -> fromSqlIndexDeclaration((SqlIndexDeclaration) col))
            .collect(Collectors.toCollection(ArrayList::new));

        indexList.addAll(create.columnList.stream()
            .filter(SqlKeyConstraint.class::isInstance)
            .map(SqlKeyConstraint.class::cast)
            .filter(constraint -> constraint.getOperator().getKind() == SqlKind.UNIQUE)
            .map(DingoDdlExecutor::fromSqlKeyConstraint).collect(Collectors.toList()));
        return indexList;
    }

    private static Index fromSqlKeyConstraint(SqlKeyConstraint sqlKeyConstraint) {
        SqlNodeList columnList = (SqlNodeList) sqlKeyConstraint.getOperandList().get(1);
        String[] columns = columnList.getList().stream()
            .filter(Objects::nonNull)
            .map(SqlIdentifier.class::cast)
            .map(SqlIdentifier::getSimple)
            .map(String::toUpperCase)
            .toArray(String[]::new);
        SqlIdentifier name = (SqlIdentifier) sqlKeyConstraint.getOperandList().get(0);
        return new Index(name.names.get(0).toUpperCase(), columns, true);
    }

    private static @Nullable Index fromSqlIndexDeclaration(
        @NonNull SqlIndexDeclaration indexDeclaration
    ) {
        String[] columns = indexDeclaration.columnList.getList().stream()
                .filter(Objects::nonNull)
                .map(SqlIdentifier.class::cast)
                .map(SqlIdentifier::getSimple)
                .map(String::toUpperCase)
                .toArray(String[]::new);
        return new Index(indexDeclaration.index, columns, false);
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
        int scale = typeName.allowsScale() ? dataType.getScale() : RelDataType.SCALE_NOT_SPECIFIED;
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

        // Obtaining id from method
        if (scd.isAutoIncrement()) {
            defaultValue = "";
        }

        String name = scd.name.getSimple().toUpperCase();
        if (!dataType.isNullable() && "NULL".equalsIgnoreCase(defaultValue)) {
            throw DINGO_RESOURCE.invalidDefaultValue(name).ex();
        }
        assert pkSet != null;
        int primary = pkSet.indexOf(name);
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
            .build();
    }

    private static @NonNull Pair<MutableSchema, String> getSchemaAndTableName(
        @NonNull SqlIdentifier id, CalcitePrepare.@NonNull Context context
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
        return Pair.of((MutableSchema) schema, tableName.toUpperCase());
    }

    @SuppressWarnings({"unused"})
    public void execute(SqlCreateTable createT, CalcitePrepare.Context context) {
        DingoSqlCreateTable create = (DingoSqlCreateTable) createT;
        log.info("DDL execute: {}", create);
        final Pair<MutableSchema, String> schemaTableName
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
            ).filter(ks -> !ks.isEmpty())
            .orElseThrow(() -> DINGO_RESOURCE.primaryKeyRequired(tableName).ex());
        SqlValidator validator = new ContextSqlValidator(context, true);

        // Mapping, column node -> column definition
        List<ColumnDefinition> columns = create.columnList.stream()
            .filter(col -> col.getKind() == SqlKind.COLUMN_DECL)
            .map(col -> fromSqlColumnDeclaration((DingoSqlColumn) col, validator, pks))
            .collect(Collectors.toCollection(ArrayList::new));


        // Distinct column
        long distinctColCnt = columns.stream().map(ColumnDefinition::getName).distinct().count();
        long realColCnt = columns.size();
        if (distinctColCnt != realColCnt) {
            throw new RuntimeException("Duplicate column names are not allowed in table definition. Total: "
                + realColCnt + ", distinct: " + distinctColCnt);
        }

        final MutableSchema schema = Parameters.nonNull(schemaTableName.left, "table schema");

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
            create.getOriginalCreateSql()
        );
        List<Index> indexList = getIndex(create);

        // Validate partition strategy
        Optional.ifPresent(create.getPartDefinition(), __ -> validatePartitionBy(pks, tableDefinition, __));

        schema.createTable(tableName, tableDefinition);
        if (indexList.size() > 0) {
            schema.createIndex(tableName, indexList);
        }
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

        env.getTableIdMap().computeIfPresent(schema.metaService.id(), (k, v) -> {
            v.remove(tableName);
            return v;
        });
    }

    public void execute(@NonNull SqlTruncate truncate, CalcitePrepare.Context context) {
        SqlIdentifier name = (SqlIdentifier) truncate.getOperandList().get(0);
        final Pair<MutableSchema, String> schemaTableName
            = getSchemaAndTableName(name, context);
        final MutableSchema schema = Parameters.nonNull(schemaTableName.left, "table schema");
        final String tableName = Parameters.nonNull(schemaTableName.right, "table name");
        TableDefinition tableDefinition = schema.getMetaService().getTableDefinition(tableName.toUpperCase());
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
        schema.createTable(tableName, tableDefinition);
    }


    public void execute(@NonNull SqlGrant sqlGrant, CalcitePrepare.Context context) {
        log.info("DDL execute: {}", sqlGrant);
        if (!"*".equals(sqlGrant.table)) {
            SqlIdentifier name = sqlGrant.tableIdentifier;
            final Pair<MutableSchema, String> schemaTableName
                = getSchemaAndTableName(name, context);
            final String tableName = Parameters.nonNull(schemaTableName.right, "table name");
            final MutableSchema schema = Parameters.nonNull(schemaTableName.left, "table schema");
            if (schema.getTable(tableName) == null) {
                throw new RuntimeException("table doesn't exist");
            }
        }
        sqlGrant.host = getRealAddress(sqlGrant.host);
        if (userService.existsUser(UserDefinition.builder().user(sqlGrant.user)
            .host(sqlGrant.host).build())) {
            PrivilegeDefinition privilegeDefinition = getPrivilegeDefinition(sqlGrant, context);
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
        final Pair<MutableSchema, String> schemaTableName
            = getSchemaAndTableName(sqlAlterTableDistribution.table, context);
        final String tableName = Parameters.nonNull(schemaTableName.right, "table name");
        MutableSchema schema = Parameters.nonNull(schemaTableName.left, "table schema");
        TableDefinition tableDefinition = schema.getMetaService().getTableDefinition(tableName);
        PartitionDetailDefinition detail = sqlAlterTableDistribution.getPartitionDefinition();
        DefinitionUtils.checkAndConvertRangePartitionDetail(tableDefinition, detail);
        schema.addDistribution(tableName, sqlAlterTableDistribution.getPartitionDefinition());
    }

    public void execute(@NonNull SqlAlterAddIndex sqlAlterAddIndex, CalcitePrepare.Context context) {
        final Pair<MutableSchema, String> schemaTableName
            = getSchemaAndTableName(sqlAlterAddIndex.table, context);
        final String tableName = Parameters.nonNull(schemaTableName.right, "table name");
        final MutableSchema schema = Parameters.nonNull(schemaTableName.left, "table schema");
        Index index = new Index(sqlAlterAddIndex.index, sqlAlterAddIndex.getColumnNames(), sqlAlterAddIndex.isUnique);
        validateIndex(schema, tableName, index);
        schema.createIndex(tableName, Collections.singletonList(index));
    }

    public void execute(@NonNull SqlCreateIndex sqlCreateIndex, CalcitePrepare.Context context) {
        final Pair<MutableSchema, String> schemaTableName
            = getSchemaAndTableName(sqlCreateIndex.table, context);
        final String tableName = Parameters.nonNull(schemaTableName.right, "table name");
        final MutableSchema schema = Parameters.nonNull(schemaTableName.left, "table schema");
        Index index = new Index(sqlCreateIndex.index, sqlCreateIndex.getColumnNames(), sqlCreateIndex.isUnique);
        validateIndex(schema, tableName, index);
        schema.createIndex(tableName, Arrays.asList(index));
    }

    public void execute(@NonNull SqlDropIndex sqlDropIndex, CalcitePrepare.Context context) {
        final Pair<MutableSchema, String> schemaTableName
            = getSchemaAndTableName(sqlDropIndex.table, context);
        final String tableName = Parameters.nonNull(schemaTableName.right, "table name");
        final MutableSchema schema = Parameters.nonNull(schemaTableName.left, "table schema");
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

    public void execute(@NonNull SqlSetOption sqlSetOption, CalcitePrepare.Context context) {
        log.info("sql set option");
    }

    public void execute(@NonNull SqlCommit sqlCommit, CalcitePrepare.Context context) {
        log.info("commit");
    }

    public void execute(@NonNull SqlRollback sqlRollback, CalcitePrepare.Context context) {
        log.info("rollback");
    }

    public void execute(@NonNull SqlUseSchema sqlUseSchema, CalcitePrepare.Context context) {
        log.info("use schema");
    }

    public void validateDropIndex(MutableSchema schema, String tableName, String indexName) {
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

    public void validateIndex(MutableSchema schema, String tableName, Index newIndex) {
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
            case "RANGE":
                DefinitionUtils.checkAndConvertRangePartition(tableDefinition);
                break;
            default:
                throw new IllegalStateException("Unsupported " + strategy);
        }
    }

    @NonNull
    private PrivilegeDefinition getPrivilegeDefinition(
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
            if (context.getRootSchema().getSubSchema(schemaName, true) == null) {
                throw new RuntimeException("schema " + schemaName + " does not exist");
            }
            privilegeDefinition = SchemaPrivDefinition.builder()
                .schemaName(schemaName)
                .build();
            privilegeType = PrivilegeType.SCHEMA;
        } else {
            CalciteSchema schema = context.getRootSchema().getSubSchema(schemaName, true);
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
