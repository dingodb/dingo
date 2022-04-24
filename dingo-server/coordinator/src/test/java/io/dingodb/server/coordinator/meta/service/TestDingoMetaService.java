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

package io.dingodb.server.coordinator.meta.service;

import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.server.coordinator.TestBase;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static org.assertj.core.api.Assertions.assertThat;

public class TestDingoMetaService extends TestBase {

    public static final Path DATA_PATH = Paths.get(TestDingoMetaService.class.getName());

    private static TableDefinition definition;

    @BeforeAll
    public static void beforeAll() throws Exception {
        afterAll();
        TestBase.beforeAll(DATA_PATH);
        definition = new TableDefinition("TestTableAdaptor_testCreate");
        definition.setColumns(Arrays.asList(
            ColumnDefinition.builder().name("col1").type(SqlTypeName.INTEGER).build(),
            ColumnDefinition.builder().name("col2").type(SqlTypeName.VARCHAR).precision(32).build(),
            ColumnDefinition.builder().name("col3").type(SqlTypeName.INTEGER).build()
        ));
    }

    @AfterAll
    public static void afterAll() throws Exception {
        TestBase.afterAll(DATA_PATH);
    }

    @AfterEach
    public void afterEach() {
        LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
    }

    @Test
    public void test00Create() {
        DingoMetaService.instance().createTable(definition.getName(), definition);
        assertThat(DingoMetaService.instance().getTableDefinition(definition.getName()))
            .isEqualTo(definition);
    }

    @Test
    public void test01GetParts() {
        assertThat(DingoMetaService.instance().getParts(definition.getName()))
            .hasSizeGreaterThanOrEqualTo(1);
        assertThat(DingoMetaService.instance().getDistributes(definition.getName()))
            .hasSizeGreaterThanOrEqualTo(1);
    }

}
