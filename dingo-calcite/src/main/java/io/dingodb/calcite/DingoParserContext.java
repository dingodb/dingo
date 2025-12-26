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

import com.google.common.collect.Multimap;
import io.dingodb.calcite.grammar.SqlUserDefinedOperators;
import io.dingodb.calcite.schema.RootCalciteSchema;
import io.dingodb.calcite.schema.RootSnapshotSchema;
import io.dingodb.calcite.type.DingoSqlTypeFactory;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.mysql.SQLMode;
import io.dingodb.common.time.DingoTimeZoneContext;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.Context;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;
import org.apache.calcite.sql2rel.SqlDiskAnnOperator;
import org.apache.calcite.sql2rel.SqlDocumentOperator;
import org.apache.calcite.sql2rel.SqlFunctionScanOperator;
import org.apache.calcite.sql2rel.SqlGenerateSeriesOperator;
import org.apache.calcite.sql2rel.SqlHybridSearchOperator;
import org.apache.calcite.sql2rel.SqlLikeBinaryOperator;
import org.apache.calcite.sql2rel.SqlVectorOperator;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Field;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;

import static io.dingodb.calcite.DingoParser.PARSER_CONFIG;
import static io.dingodb.common.util.NameCaseUtils.caseSensitive;
import static io.dingodb.common.util.NameCaseUtils.convertName;

@Slf4j
// These are static for every sql parsing.
public final class DingoParserContext implements Context {
    @Getter
    private final DingoCatalogReader catalogReader;
    @Getter
    private final RootCalciteSchema rootSchema;
    @Getter
    private String defaultSchemaName;
    @Getter
    private final CalciteConnectionConfig config;
    @Getter
    @Setter
    private TimeZone timeZone;
    @Getter
    private final boolean usingRelOp;
    @Getter
    private final boolean pushDown;

    @Getter
    private final Properties options;
    @Getter
    private CalciteSchema usedSchema;

    @Setter
    @Getter
    private List<SQLWarning> warningList = new ArrayList<>();

    @Getter
    private final Properties sessionVariables;

    @Getter
    @Setter
    private boolean autoCommit;

    public DingoParserContext(@NonNull String defaultSchemaName) {
        this(defaultSchemaName, null, null, TimeZone.getDefault());
    }

    public DingoParserContext(@NonNull String defaultSchemaName, @Nullable Properties options,
                              @Nullable Properties sessionVariables) {
        this(defaultSchemaName, options, sessionVariables, TimeZone.getDefault());
    }

    public DingoParserContext(@NonNull String defaultSchemaName, @Nullable Properties options,
                              @Nullable Properties sessionVariables, TimeZone timeZone) {
        this.defaultSchemaName = defaultSchemaName;
        this.sessionVariables = sessionVariables;
        this.autoCommit = false;

        timeZone = timeZone == null ? TimeZone.getDefault() : timeZone;
        this.timeZone = timeZone;

        DingoTimeZoneContext.setTimeZone(timeZone);

        String usingRelOpStr = options != null ? options.getProperty("usingRelOp") : null;
        usingRelOp = (usingRelOpStr == null || Boolean.parseBoolean(usingRelOpStr));

        String pushDownStr = (options != null ? options.getProperty("pushDown") : null);
        pushDown = (pushDownStr == null || Boolean.parseBoolean(pushDownStr));

        rootSchema = RootCalciteSchema.builder()
            .schema(new RootSnapshotSchema(this))
            .name(RootSnapshotSchema.ROOT_SCHEMA_NAME)
            .build();
        LogUtils.info(log, "init rootCalcite:{}, this:{}, defaultSchemaName:{}", rootSchema,
            this, defaultSchemaName);

        RelProtoDataType mapType = (RelDataTypeFactory factory) -> factory.createSqlType(SqlTypeName.ANY);
        rootSchema.add("map", mapType);
        RelProtoDataType blobType = (RelDataTypeFactory factory) -> factory.createSqlType(SqlTypeName.VARBINARY);
        rootSchema.add("blob", blobType);
        rootSchema.add("longblob", blobType);
        rootSchema.add("mediumblob", blobType);
        rootSchema.add("tinyblob", blobType);
        RelProtoDataType stringType = (RelDataTypeFactory factory) -> factory.createSqlType(SqlTypeName.VARCHAR);
        rootSchema.add("json", stringType);

        CalciteConnectionConfigImpl config;
        if (options != null) {
            config = new CalciteConnectionConfigImpl(options);
        } else {
            config = new CalciteConnectionConfigImpl(new Properties());
        }
        this.config = config
            .set(CalciteConnectionProperty.CASE_SENSITIVE, String.valueOf(PARSER_CONFIG.caseSensitive()));
        // Create CatalogReader.
        List<String> schemas = new ArrayList<>();
        schemas.add(defaultSchemaName);
        List<List<String>> schemaPaths = new ArrayList<>();
        schemaPaths.add(schemas);
        schemaPaths.add(new ArrayList<>());
        catalogReader = new DingoCatalogReader(
            rootSchema,
            schemaPaths,
            getTypeFactory(),
            this.config
        );
        if (sessionVariables != null) {
            catalogReader.setSqlModeFlags(SQLMode.convertToFlag(sessionVariables.getProperty("sql_mode")));
        }

        // Register operators
        SqlStdOperatorTable tableInstance = SqlStdOperatorTable.instance();
        tableInstance.register(SqlUserDefinedOperators.LIKE_BINARY);
        tableInstance.register(SqlUserDefinedOperators.NOT_LIKE_BINARY);
        tableInstance.register(SqlUserDefinedOperators.SCAN);
        tableInstance.register(SqlUserDefinedOperators.TEXT_SEARCH);
        tableInstance.register(SqlUserDefinedOperators.HYBRID_SEARCH);
        tableInstance.register(SqlUserDefinedOperators.DISK_ANN_BUILD);
        tableInstance.register(SqlUserDefinedOperators.DISK_ANN_LOAD);
        tableInstance.register(SqlUserDefinedOperators.DISK_ANN_STATUS);
        tableInstance.register(SqlUserDefinedOperators.DISK_ANN_COUNT_MEMORY);
        tableInstance.register(SqlUserDefinedOperators.DISK_ANN_RESET);
        tableInstance.register(SqlUserDefinedOperators.GENERATE_SERIES);
        SqlLikeBinaryOperator.register();
        SqlFunctionScanOperator.register(this);
        SqlVectorOperator.register(this);
        SqlDocumentOperator.register(this);
        SqlHybridSearchOperator.register(this);
        SqlDiskAnnOperator.register(this);
        SqlGenerateSeriesOperator.register(this);
        // select user from user ; user is default special operator
        eliminateUserOperator(tableInstance);

        this.options = options;

        usedSchema = getDefaultSchema();
    }

    protected void resetSchemaCache() {
        // Not a good way, need to find a better way to clear the root mode cache?
        rootSchema.setCache(false);
        rootSchema.setCache(true);
    }

    private static void eliminateUserOperator(SqlStdOperatorTable tableInstance) {
        try {
            Field caseSensitiveOperators = ReflectiveSqlOperatorTable
                .class.getDeclaredField("caseSensitiveOperators");
            Field caseInsensitiveOperators = ReflectiveSqlOperatorTable
                .class.getDeclaredField("caseInsensitiveOperators");
            caseSensitiveOperators.setAccessible(true);
            caseInsensitiveOperators.setAccessible(true);
            Multimap caseSensitiveMap = (Multimap) caseSensitiveOperators.get(tableInstance);
            Multimap caseInsensitiveMap = (Multimap) caseInsensitiveOperators.get(tableInstance);

            Iterator<Map.Entry> caseSensitiveIterator = caseSensitiveMap.entries().iterator();
            while (caseSensitiveIterator.hasNext()) {
                Map.Entry entry = caseSensitiveIterator.next();
                if (entry.getValue() == SqlStdOperatorTable.USER) {
                    caseSensitiveIterator.remove();
                }
            }

            Iterator<Map.Entry> caseInsensitiveIterator = caseInsensitiveMap.entries().iterator();
            while (caseInsensitiveIterator.hasNext()) {
                Map.Entry entry = caseInsensitiveIterator.next();
                if (entry.getValue() == SqlStdOperatorTable.USER) {
                    caseInsensitiveIterator.remove();
                }
            }
        } catch (Exception ignored) {
        }
    }

    @SuppressWarnings("MethodMayBeStatic")
    public JavaTypeFactory getTypeFactory() {
        return DingoSqlTypeFactory.INSTANCE;
    }

    /**
     * Get a {@link DingoSqlValidator}. Sql validation must begin with a new sql validator.
     *
     * @return the sql validator
     */
    public @NonNull DingoSqlValidator getSqlValidator() {
        // Create SqlValidator.
        // CatalogReader is also serving as SqlOperatorTable.
        return new DingoSqlValidator(catalogReader, getTypeFactory());
    }

    public CalciteSchema getDefaultSchema() {
        return rootSchema.getSubSchema(defaultSchemaName, caseSensitive());
    }

    public CalciteSchema getSchemaByNames(@NonNull List<String> names) {
        // ignore 0 root schema
        return rootSchema.getSubSchema(convertName(names.get(1)), caseSensitive());
    }

    @Override
    public <C> @Nullable C unwrap(@NonNull Class<C> clazz) {
        if (clazz.isInstance(timeZone)) {
            return clazz.cast(timeZone);
        }
        return null;
    }

    public String getOption(String field) {
        return options != null ? options.getOrDefault(field, "").toString() : "";
    }

    public boolean keyExists(String field) {
        return options != null && options.containsKey(field);
    }

    public void addWarning(SQLWarning warning) {
        this.getWarningList().clear();
        if (warning != null) {
            this.getWarningList().add(warning);
        }
    }

    public synchronized void setUsedSchema(CalciteSchema schema) {
        this.usedSchema = schema;
        LogUtils.info(log, "set Used schema:{}, origin default schema:{}",
            schema.getName(), this.defaultSchemaName);
        this.defaultSchemaName = schema.getName();
        boolean exists = false;
        for (List<String> item : catalogReader.getSchemaPaths()) {
            if (item.contains(schema.getName())) {
                exists = true;
            }
        }
        if (!exists) {
            int size = catalogReader.getSchemaPaths().size();
            if (size == 2) {
                List<String> array = catalogReader.getSchemaPaths().get(0);
                array.set(0, schema.getName());
            }
        }
    }

    public void setSqlNode(String sqlNodeStr) {
        this.catalogReader.setSqlModeFlags(SQLMode.convertToFlag(sqlNodeStr));
    }
}
