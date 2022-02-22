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

import lombok.Getter;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.prepare.CalciteCatalogReader;

import java.util.Collections;

// These are static for every sql parsing.
public final class DingoParserContext {
    @Getter
    private final JavaTypeFactory typeFactory;
    @Getter
    private final CalciteSchema rootSchema;
    @Getter
    private final CalciteCatalogReader catalogReader;

    public DingoParserContext() {
        typeFactory = new JavaTypeFactoryImpl();
        rootSchema = CalciteSchema.createRootSchema(
            true,
            false,
            DingoSchema.SCHEMA_NAME,
            DingoSchema.ROOT
        );
        catalogReader = new CalciteCatalogReader(
            rootSchema,
            Collections.singletonList(DingoSchema.SCHEMA_NAME),
            typeFactory,
            CalciteConnectionConfig.DEFAULT
        );
    }
}
