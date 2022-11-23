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

import io.dingodb.calcite.type.DingoSqlTypeFactory;
import lombok.Getter;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.Context;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Properties;
import java.util.TimeZone;


// These are static for every sql parsing.
public final class DingoParserContext implements Context {
    @Getter
    private final CalciteSchema rootSchema;
    @Getter
    private final String defaultSchemaName;
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
            DingoRootSchema.ROOT
        );

        RelProtoDataType mapType = (RelDataTypeFactory factory) -> factory.createSqlType(SqlTypeName.ANY);
        rootSchema.add("map", mapType);

        // Register all the functions
        /*
        DingoFunctions.getInstance().getDingoFunctions().forEach(method -> {
            rootSchema.plus().add(method.getName().toUpperCase(), ScalarFunctionImpl.create(method.getMethod()));
        });
         */
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
