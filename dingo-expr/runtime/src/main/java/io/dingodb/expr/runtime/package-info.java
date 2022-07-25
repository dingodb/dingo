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
@GenerateTypeCodes({
    // scalar types
    @GenerateTypeCodes.TypeCode(name = "BOOL", type = "java.lang.Boolean",
        aliases = {"BOOLEAN"}),
    @GenerateTypeCodes.TypeCode(name = "INT", type = "java.lang.Integer",
        aliases = {"INTEGER", "TINYINT"}),
    @GenerateTypeCodes.TypeCode(name = "LONG", type = "java.lang.Long",
        aliases = {"BIGINT"}),
    @GenerateTypeCodes.TypeCode(name = "DOUBLE", type = "java.lang.Double",
        aliases = {"FLOAT", "REAL"}),
    @GenerateTypeCodes.TypeCode(name = "STRING", type = "java.lang.String",
        aliases = {"CHAR", "VARCHAR"}),
    @GenerateTypeCodes.TypeCode(name = "DECIMAL", type = "java.math.BigDecimal"),
    @GenerateTypeCodes.TypeCode(name = "BINARY", type = "byte[]",
        aliases = {"VARBINARY", "BLOB"}),
    @GenerateTypeCodes.TypeCode(name = "OBJECT", type = "java.lang.Object",
        aliases = {"ANY"}),
    // array types
    @GenerateTypeCodes.TypeCode(name = "BOOL_ARRAY", type = "java.lang.Boolean[]",
        aliases = {"BOOLEAN_ARRAY"}),
    @GenerateTypeCodes.TypeCode(name = "INT_ARRAY", type = "java.lang.Integer[]",
        aliases = {"INTEGER_ARRAY"}),
    @GenerateTypeCodes.TypeCode(name = "LONG_ARRAY", type = "java.lang.Long[]",
        aliases = {"BIGINT_ARRAY"}),
    @GenerateTypeCodes.TypeCode(name = "DOUBLE_ARRAY", type = "java.lang.Double[]"),
    @GenerateTypeCodes.TypeCode(name = "STRING_ARRAY", type = "java.lang.String[]",
        aliases = {"CHAR_ARRAY", "VARCHAR_ARRAY"}),
    @GenerateTypeCodes.TypeCode(name = "DECIMAL_ARRAY", type = "java.math.BigDecimal[]"),
    @GenerateTypeCodes.TypeCode(name = "OBJECT_ARRAY", type = "java.lang.Object[]",
        aliases = {"ANY_ARRAY"}),
    // collection types
    @GenerateTypeCodes.TypeCode(name = "LIST", type = "java.util.List"),
    @GenerateTypeCodes.TypeCode(name = "MAP", type = "java.util.Map"),
    // date&time types
    @GenerateTypeCodes.TypeCode(name = "DATE", type = "java.sql.Date"),
    @GenerateTypeCodes.TypeCode(name = "TIME", type = "java.sql.Time"),
    @GenerateTypeCodes.TypeCode(name = "TIMESTAMP", type = "java.sql.Timestamp"),
    // pseudo types
    @GenerateTypeCodes.TypeCode(name = "TUPLE", type = "tuple"),
    @GenerateTypeCodes.TypeCode(name = "DICT", type = "dict"),
})
package io.dingodb.expr.runtime;

import io.dingodb.expr.annotations.GenerateTypeCodes;
