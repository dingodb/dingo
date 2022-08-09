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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

public class TestCollectionValuesPlan {
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

    @Test
    public void testArray() throws SqlParseException {
        RelNode relNode = parse("select array[1, 2, 3]");
        DingoValues values = (DingoValues) Assert.relNode(relNode)
            .isA(DingoValues.class).convention(DingoConventions.ROOT)
            .getInstance();
        assertThat(values.getTuples()).containsExactly(
            new Object[]{Arrays.asList(1, 2, 3)}
        );
    }

    @Test
    public void testMultiSet() throws SqlParseException {
        RelNode relNode = parse("select multiset[1, 2, 3]");
        DingoValues values = (DingoValues) Assert.relNode(relNode)
            .isA(DingoValues.class).convention(DingoConventions.ROOT)
            .getInstance();
        assertThat(values.getTuples()).containsExactly(
            new Object[]{Arrays.asList(1, 2, 3)}
        );
    }
}
