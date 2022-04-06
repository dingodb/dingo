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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class TestDingoParser {
    private static DingoParser parser;

    @BeforeAll
    public static void setupAll() {
        DingoParserContext context = new DingoParserContext(MockMetaServiceProvider.SCHEMA_NAME);
        parser = new DingoParser(context);
    }

    private static SqlNode parse(String sql) throws SqlParseException {
        SqlNode sqlNode = parser.parse(sql);
        return parser.validate(sqlNode);
    }

    @Test
    public void testGetValidatedNodeType() throws SqlParseException {
        SqlNode sqlNode = parse("select id, name, amount from test");
        RelDataType type = parser.getValidatedNodeType(sqlNode);
        RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        assertThat(type.toString()).isEqualTo(
            typeFactory.createStructType(
                ImmutableList.of(
                    typeFactory.createSqlType(SqlTypeName.INTEGER),
                    typeFactory.createSqlType(SqlTypeName.VARCHAR, 64),
                    typeFactory.createSqlType(SqlTypeName.DOUBLE)
                ),
                ImmutableList.of(
                    "ID",
                    "NAME",
                    "AMOUNT"
                )
            ).toString()
        );
    }

    @Test
    public void testGetFieldOrigins() throws SqlParseException {
        SqlNode sqlNode = parse("select id, name, amount from test");
        List<List<String>> fieldOrigins = parser.getFieldOrigins(sqlNode);
        assertThat(fieldOrigins).isEqualTo(ImmutableList.of(
            ImmutableList.of("DINGO_ROOT", "MOCK", "TEST", "ID"),
            ImmutableList.of("DINGO_ROOT", "MOCK", "TEST", "NAME"),
            ImmutableList.of("DINGO_ROOT", "MOCK", "TEST", "AMOUNT")
        ));
    }
}
