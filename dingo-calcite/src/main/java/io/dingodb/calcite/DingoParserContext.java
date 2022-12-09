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
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlLikeBinaryOperator;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collections;
import java.util.Properties;
import java.util.TimeZone;

import static io.dingodb.calcite.DingoParser.PARSER_CONFIG;


// These are static for every sql parsing.
public final class DingoParserContext implements Context {
    @Getter
    private final CalciteCatalogReader catalogReader;
    @Getter
    private final SqlValidator sqlValidator;
    @Getter
    private final CalciteSchema rootSchema;
    @Getter
    private final String defaultSchemaName;
    @Getter
    private final CalciteConnectionConfig config;
    @Getter
    private final TimeZone timeZone;

    public DingoParserContext(@NonNull String defaultSchemaName) {
        this(defaultSchemaName, null);
    }

    public DingoParserContext(@NonNull String defaultSchemaName, @Nullable Properties options) {
        this.defaultSchemaName = defaultSchemaName;

        String timeZoneId = options != null ? options.getProperty("timeZone") : null;
        timeZone = timeZoneId != null ? TimeZone.getTimeZone(timeZoneId) : TimeZone.getDefault();

        rootSchema = CalciteSchema.createRootSchema(
            true,
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
        catalogReader = new CalciteCatalogReader(
            rootSchema,
            Collections.singletonList(defaultSchemaName),
            getTypeFactory(),
            this.config
        );

        // Create SqlValidator.
        // CatalogReader is also serving as SqlOperatorTable.
        SqlStdOperatorTable tableInstance = SqlStdOperatorTable.instance();
        // Register operators
        tableInstance.register(SqlUserDefinedOperators.LIKE_BINARY);
        tableInstance.register(SqlUserDefinedOperators.NOT_LIKE_BINARY);
        SqlLikeBinaryOperator.register();

        sqlValidator = new DingoSqlValidator(catalogReader, getTypeFactory());
    }

    @SuppressWarnings("MethodMayBeStatic")
    public JavaTypeFactory getTypeFactory() {
        return DingoSqlTypeFactory.INSTANCE;
    }

    public CalciteSchema getDefaultSchema() {
        return rootSchema.getSubSchema(defaultSchemaName, true);
    }

    @Override
    public <C> @Nullable C unwrap(@NonNull Class<C> clazz) {
        if (clazz.isInstance(timeZone)) {
            return clazz.cast(timeZone);
        }
        return null;
    }
}
