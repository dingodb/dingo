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
import org.apache.calcite.sql.validate.SqlValidator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

public class TestDingoSqlValidator {
    private DingoParser parser;

    @BeforeEach
    public void setupAll() {
        MockMetaServiceProvider.init();
        Properties properties = new Properties();
        DingoParserContext context = new DingoParserContext(MockMetaServiceProvider.SCHEMA_NAME, properties);
        parser = new DingoParser(context);
    }

    @Test
    public void testGetValidatedNodeType() throws SqlParseException {
        SqlNode sqlNode = parser.parse("select id, name, amount from test");
        SqlValidator validator = parser.getSqlValidator();
        validator.validate(sqlNode);
        RelDataType type = validator.getValidatedNodeType(sqlNode);
        RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        assertThat(type.toString()).isEqualTo(
            typeFactory.createStructType(
                ImmutableList.of(
                    typeFactory.createSqlType(SqlTypeName.INTEGER),
                    typeFactory.createSqlType(SqlTypeName.VARCHAR, 64),
                    typeFactory.createSqlType(SqlTypeName.DOUBLE)
                ),
                ImmutableList.of(
                    "id",
                    "name",
                    "amount"
                )
            ).toString()
        );
    }

    @Test
    public void testGetFieldOrigins() throws SqlParseException {
        SqlNode sqlNode = parser.parse("select id, name, amount from test");
        SqlValidator validator = parser.getSqlValidator();
        validator.validate(sqlNode);
        List<List<String>> fieldOrigins = validator.getFieldOrigins(sqlNode);
        String tableName = "TEST";
        String schemaName = "DINGO";
        assertThat(fieldOrigins).isEqualTo(ImmutableList.of(
            ImmutableList.of("DINGO_ROOT", schemaName, tableName, "ID"),
            ImmutableList.of("DINGO_ROOT", schemaName, tableName, "NAME"),
            ImmutableList.of("DINGO_ROOT", schemaName, tableName, "AMOUNT")
        ));
    }
}
