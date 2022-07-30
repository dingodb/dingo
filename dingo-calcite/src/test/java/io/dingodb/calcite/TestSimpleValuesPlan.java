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

import io.dingodb.calcite.assertion.Assert;
import io.dingodb.calcite.mock.MockMetaServiceProvider;
import io.dingodb.calcite.rel.DingoValues;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.DingoTypeFactory;
import io.dingodb.common.type.converter.CsvConverter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import javax.annotation.Nonnull;

import static org.assertj.core.api.Assertions.assertThat;

public class TestSimpleValuesPlan {
    private static DingoParser parser;

    @BeforeAll
    public static void setupAll() {
        DingoParserContext context = new DingoParserContext(MockMetaServiceProvider.SCHEMA_NAME);
        parser = new DingoParser(context);
    }

    private static RelNode parse(String sql) throws SqlParseException {
        SqlNode sqlNode = parser.parse(sql);
        sqlNode = parser.validate(sqlNode);
        RelRoot relRoot = parser.convert(sqlNode);
        return parser.optimize(relRoot.rel);
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
        RelNode relNode = parse("select " + expr.replace('\"', '\''));
        DingoValues values = (DingoValues) Assert.relNode(relNode)
            .isA(DingoValues.class).convention(DingoConventions.ROOT)
            .getInstance();
        DingoType type = DingoTypeFactory.fromRelDataType(values.getRowType());
        assertThat(values.getTuples()).containsExactly(
            (Object[]) type.convertFrom(new Object[]{result}, CsvConverter.INSTANCE)
        );
    }
}
