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

package io.dingodb.test.cases.test;

import io.dingodb.test.SqlHelper;
import io.dingodb.test.cases.provider.StressCasesJUnit5;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@Slf4j
@Disabled
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class StressTest {
    private static SqlHelper sqlHelper;

    @BeforeAll
    public static void setupAll() throws Exception {
        sqlHelper = new SqlHelper();
    }

    @AfterAll
    public static void cleanUpAll() throws Exception {
        sqlHelper.cleanUp();
    }

    @Test
    public void testInsert() throws Exception {
        new StressCasesJUnit5().insert(sqlHelper.getConnection());
    }

    @Test
    public void testInsertParameters() throws Exception {
        new StressCasesJUnit5().insertWithParameters(sqlHelper.getConnection());
    }

    @Test
    public void testInsertParametersBatch() throws Exception {
        new StressCasesJUnit5().insertWithParametersBatch(sqlHelper.getConnection());
    }
}
