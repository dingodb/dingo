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

package io.dingodb.test;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.SQLException;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@Slf4j
public class QueryNoTableTest {
    private static SqlHelper sqlHelper;

    @BeforeAll
    public static void setupAll() throws Exception {
        sqlHelper = new SqlHelper();
    }

    @AfterAll
    public static void cleanUpAll() throws Exception {
        sqlHelper.cleanUp();
    }

    @Nonnull
    public static Stream<Arguments> getSimpleValuesTestParameters() {
        return Stream.of(
            arguments("select 'hello'", "hello"),
            arguments("select 'abc' || null", null),
            arguments("select date_format('', '%Y-%m-%d')", null),
            arguments("select datediff('', '')", null)
        );
    }

    @ParameterizedTest
    @MethodSource("getSimpleValuesTestParameters")
    public void testSimpleValues(String sql, Object result) throws SQLException {
        // Queries like 'select 1' is bypassed by Calcite.
        assertThat(sqlHelper.querySingleValue(sql)).isEqualTo(result);
    }
}
