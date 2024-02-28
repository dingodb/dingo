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

import com.google.common.collect.ImmutableList;
import io.dingodb.calcite.mock.MockMetaServiceProvider;
import io.dingodb.calcite.rel.dingo.DingoRoot;
import io.dingodb.calcite.rel.DingoValues;
import io.dingodb.calcite.traits.DingoRelStreaming;
import io.dingodb.calcite.type.converter.DefinitionMapper;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.converter.StrParseConverter;
import io.dingodb.test.asserts.Assert;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.Date;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestValues {
    private static DingoParserContext context;
    private DingoParser parser;

    @BeforeAll
    public static void setupAll() {
        MockMetaServiceProvider.init();
        Properties properties = new Properties();
        context = new DingoParserContext(MockMetaServiceProvider.SCHEMA_NAME, properties);
    }

    private static void assertSelectValues(SqlNode sqlNode) {
        Assert.sqlNode(sqlNode).kind(SqlKind.SELECT);
        SqlSelect select = (SqlSelect) sqlNode;
        Assert.sqlNode(select.getFrom()).isNull();
    }

    @Nonnull
    public static Stream<Arguments> getSelectParameters() {
        return Stream.of(
            arguments(
                "select array[1, 2, 3]",
                ImmutableList.of(new Object[]{Arrays.asList(1, 2, 3)})
            ),
            arguments(
                "select multiset[1, 2, 3]",
                ImmutableList.of(new Object[]{Arrays.asList(1, 2, 3)})
            )
        );
    }

    @Nonnull
    public static Stream<Arguments> getSelectAsParameters() {
        return Stream.of(
            arguments(
                "select a - b from (values (1, 2), (3, 5), (7, 11)) as t (a, b) where a + b > 4",
                ImmutableList.of(new Object[]{-2}, new Object[]{-4})
            ),
            arguments(
                "select b from (values (1, 2), (null, 5), (7, 11)) as t (a, b) where a is null",
                ImmutableList.of(new Object[]{5})
            ),
            arguments(
                "select b from (values (1, 2), (null, 5), (7, 11)) as t (a, b) where a is not null",
                ImmutableList.of(new Object[]{2}, new Object[]{11})
            ),
            arguments(
                "select cast(a as date) from (values('1970-1-1')) as t (a)",
                ImmutableList.of(new Object[]{new Date(0L)})
            ));
    }

    private DingoValues getDingoValues(SqlNode sqlNode) {
        RelRoot relRoot = parser.convert(sqlNode);
        RelNode optimized = parser.optimize(relRoot.rel);
        return (DingoValues) Assert.relNode(optimized)
            .isA(DingoRoot.class).streaming(DingoRelStreaming.ROOT)
            .soleInput().isA(DingoValues.class)
            .getInstance();
    }

    @BeforeEach
    public void setup() {
        // Create each time to clean the statistic info.
        parser = new DingoParser(context);
    }

    @ParameterizedTest
    @CsvSource(value = {
        "1, 1",
        "\"hello\", hello",
        "1.0, 1.0",
        "true, true",
        "false, false",
        "null, null",
    })
    public void testSimple(@Nonnull String expr, String result) throws SqlParseException {
        SqlNode sqlNode = parser.parse("select " + expr.replace('\"', '\''));
        assertSelectValues(sqlNode);
        DingoValues values = getDingoValues(sqlNode);
        DingoType type = DefinitionMapper.mapToDingoType(values.getRowType());
        assertThat(values.getTuples()).containsExactly(
            (Object[]) type.convertFrom(new Object[]{result}, StrParseConverter.INSTANCE)
        );
    }

    @ParameterizedTest
    @MethodSource("getSelectParameters")
    public void testSelect(String sql, @Nonnull List<Object[]> tuples) throws SqlParseException {
        SqlNode sqlNode = parser.parse(sql);
        assertSelectValues(sqlNode);
        DingoValues values = getDingoValues(sqlNode);
        assertThat(values.getTuples()).containsExactlyInAnyOrderElementsOf(tuples);
    }

    @ParameterizedTest
    @MethodSource("getSelectAsParameters")
    public void testSelectAs(String sql, @Nonnull List<Object[]> tuples) throws Exception {
        SqlNode sqlNode = parser.parse(sql);
        Assert.sqlNode(sqlNode).kind(SqlKind.SELECT);
        SqlSelect select = (SqlSelect) sqlNode;
        Assert.sqlNode(select.getFrom()).kind(SqlKind.AS);
        DingoValues values = getDingoValues(sqlNode);
        assertThat(values.getTuples()).containsExactlyInAnyOrderElementsOf(tuples);
    }
}
