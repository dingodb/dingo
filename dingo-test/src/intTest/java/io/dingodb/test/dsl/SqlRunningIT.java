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

package io.dingodb.test.dsl;

import io.dingodb.driver.client.DingoDriverClient;
import io.dingodb.test.dsl.builder.SqlTestCaseYamlBuilder;
import io.dingodb.test.dsl.run.SqlTestRunner;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;
import java.util.stream.Stream;

// Before run this, you must set up your cluster.
public class SqlRunningIT extends SqlTestRunner {
    public Connection getConnection() throws Exception {
        Class.forName("io.dingodb.driver.client.DingoDriverClient");
        Properties properties = new Properties();
        properties.load(SqlRunningIT.class.getResourceAsStream("/intTest.properties"));
        String url = properties.getProperty("url");
        return DriverManager.getConnection(
            DingoDriverClient.CONNECT_STRING_PREFIX + "url=" + url,
            properties
        );
    }

    @TestFactory
    public Stream<DynamicTest> testAggregation() {
        return getTests(SqlTestCaseYamlBuilder.of("cases/aggregation.yml"));
    }

    @TestFactory
    public Stream<DynamicTest> testJoin() {
        return getTests(SqlTestCaseYamlBuilder.of("cases/join.yml"));
    }

    @TestFactory
    public Stream<DynamicTest> testJoin3Tables() {
        return getTests(SqlTestCaseYamlBuilder.of("cases/join_3_tables.yml"));
    }

    @TestFactory
    public Stream<DynamicTest> testQuery() {
        return getTests(SqlTestCaseYamlBuilder.of("cases/query.yml"));
    }

    @TestFactory
    public Stream<DynamicTest> testSubQuery() {
        return getTests(SqlTestCaseYamlBuilder.of("cases/sub_query.yml"));
    }

    @TestFactory
    public Stream<DynamicTest> testBasicDml() {
        return getTests(new BasicDmlCases());
    }

    @TestFactory
    public Stream<DynamicTest> testBasicQuery() {
        return getTests(new BasicQueryCases());
    }

    @TestFactory
    public Stream<DynamicTest> testCollectionDml() {
        return getTests(new CollectionDmlCases());
    }

    @TestFactory
    public Stream<DynamicTest> testCollectionQuery() {
        return getTests(new CollectionQueryCases());
    }

    @TestFactory
    public Stream<DynamicTest> testDateTimeQuery() {
        return getTests(new DateTimeQueryCases());
    }

    @TestFactory
    public Stream<DynamicTest> testI40VsF8kQuery() {
        return getTests(SqlTestCaseYamlBuilder.of("i40_vs_f8k/query_cases.yml"));
    }

    @TestFactory
    public Stream<DynamicTest> testI4kF80Query() {
        return getTests(SqlTestCaseYamlBuilder.of("i4k_f80/query_cases.yml"));
    }

    @TestFactory
    public Stream<DynamicTest> testI4kL0Dml() {
        return getTests(SqlTestCaseYamlBuilder.of("i4k_l0/dml_cases.yml"));
    }

    @TestFactory
    public Stream<DynamicTest> testI4kVsL0Query() {
        return getTests(SqlTestCaseYamlBuilder.of("i4k_vs_l0/query_cases.yml"));
    }

    @TestFactory
    public Stream<DynamicTest> testParameterDml() {
        return getTests(new ParameterDmlCases());
    }

    @TestFactory
    public Stream<DynamicTest> testParameterQuery() {
        return getTests(new ParameterQueryCases());
    }

    @TestFactory
    public Stream<DynamicTest> testTransfer() {
        return getTests(new TransferCases());
    }

    @TestFactory
    public Stream<DynamicTest> testLike() {
        return getTests(SqlTestCaseYamlBuilder.of("i4k_vs0/query_cases_like.yml"));
    }

    @TestFactory
    public Stream<DynamicTest> testBetweenAnd() {
        return getTests(SqlTestCaseYamlBuilder.of("i4k_vs0/query_cases_between.yml"));
    }

    @TestFactory
    public Stream<DynamicTest> testDeleteRange() {
        return getTests(SqlTestCaseYamlBuilder.of("i4k_vs0/delete_range_cases.yml"));
    }

    @TestFactory
    public Stream<DynamicTest> testFunctionScan() {
        return getTests(SqlTestCaseYamlBuilder.of("i4k_vs0/function_scan_cases.yml"));
    }

    @TestFactory
    public Stream<DynamicTest> testLike1() {
        return getTests(SqlTestCaseYamlBuilder.of("i40_vsk/query_cases.yml"));
    }

    @TestFactory
    public Stream<DynamicTest> testDefaultValue() {
        return getTests(new DefaultValueCases());
    }

    @TestFactory
    public Stream<DynamicTest> testCancel() {
        return getTests(new CancelCases());
    }

    @TestFactory
    public Stream<DynamicTest> testException() {
        return getTests(new ExceptionCases());
    }

    @TestFactory
    public Stream<DynamicTest> testStressDml() {
        return getTests(new StressDmlCases());
    }
}
