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

import io.dingodb.calcite.grammar.SqlUserDefinedOperators;
import io.dingodb.calcite.type.DingoSqlTypeFactory;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.Context;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql2rel.SqlLikeBinaryOperator;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;

import static io.dingodb.calcite.DingoParser.PARSER_CONFIG;


// These are static for every sql parsing.
public final class DingoParserContext implements Context {
    @Getter
    private final DingoCatalogReader catalogReader;
    @Getter
    private final CalciteSchema rootSchema;
    @Getter
    private String defaultSchemaName;
    @Getter
    private final CalciteConnectionConfig config;
    @Getter
    private final TimeZone timeZone;
    private final Properties options;
    @Getter
    private CalciteSchema usedSchema;

    public DingoParserContext(@NonNull String defaultSchemaName) {
        this(defaultSchemaName, null);
    }

    public DingoParserContext(@NonNull String defaultSchemaName, @Nullable Properties options) {
        this.defaultSchemaName = defaultSchemaName;

        String timeZoneId = options != null ? options.getProperty("timeZone") : null;
        timeZone = timeZoneId != null ? TimeZone.getTimeZone(timeZoneId) : TimeZone.getDefault();

        rootSchema = CalciteSchema.createRootSchema(
            false,
            false,
            DingoRootSchema.ROOT_SCHEMA_NAME,
            new DingoRootSchema(this)
        );

        RelProtoDataType mapType = (RelDataTypeFactory factory) -> factory.createSqlType(SqlTypeName.ANY);
        rootSchema.add("map", mapType);

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

        // Register operators
        SqlStdOperatorTable tableInstance = SqlStdOperatorTable.instance();
        tableInstance.register(SqlUserDefinedOperators.LIKE_BINARY);
        tableInstance.register(SqlUserDefinedOperators.NOT_LIKE_BINARY);
        SqlLikeBinaryOperator.register();

        this.options = options;

        usedSchema = getDefaultSchema();
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
        return rootSchema.getSubSchema(defaultSchemaName, true);
    }

    public CalciteSchema getSchemaByNames(@NonNull List<String> names) {
        // ignore 0 root schema
        CalciteSchema schema = rootSchema;
        for (int i = 1; i < names.size() - 1; i++) {
            schema = rootSchema.getSubSchema(names.get(i), false);
        }
        return schema;
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

    public void setUsedSchema(CalciteSchema schema) {
        this.usedSchema = schema;
        this.defaultSchemaName = schema.getName();
        boolean exists = false;
        for (List<String> item : catalogReader.getSchemaPaths()) {
            if (item.contains(schema.getName())) {
                exists = true;
            }
        }
        if (!exists) {
            int size = catalogReader.getSchemaPaths().size();
            List<String> array = catalogReader.getSchemaPaths().get(size - 1);
            array.add(schema.getName());
            catalogReader.getSchemaPaths().add(new ArrayList<>());
        }
    }
}
