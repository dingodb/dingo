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

import io.dingodb.calcite.func.DingoFunc;
import io.dingodb.func.DingoFuncProvider;
import lombok.Getter;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;

import java.util.ServiceLoader;

// These are static for every sql parsing.
public final class DingoParserContext {
    @Getter
    private final JavaTypeFactory typeFactory;
    @Getter
    private final CalciteSchema rootSchema;
    @Getter
    private final String defaultSchemaName;

    public DingoParserContext() {
        this(DingoRootSchema.DEFAULT_SCHEMA_NAME);
    }

    public DingoParserContext(String defaultSchemaName) {
        this.defaultSchemaName = defaultSchemaName;
        typeFactory = new JavaTypeFactoryImpl();
        rootSchema = CalciteSchema.createRootSchema(
            true,
            false,
            DingoRootSchema.ROOT_SCHEMA_NAME,
            DingoRootSchema.ROOT
        );
        ServiceLoader.load(DingoFuncProvider.class).iterator().forEachRemaining(
            f -> f.methods().forEach(m -> rootSchema.plus().add(f.name().toUpperCase(), ScalarFunctionImpl.create(m))));
        DingoFunc.DINGO_FUNC_LIST.build().forEach((k, v) -> {
            rootSchema.plus().add(k, ScalarFunctionImpl.create(DingoFunc.class, v));
        });
    }

    public CalciteSchema getDefaultSchema() {
        return rootSchema.getSubSchema(defaultSchemaName, true);
    }
}
