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
    // date&time types
    @GenerateTypeCodes.TypeCode(name = "DATE", type = "java.sql.Date"),
    @GenerateTypeCodes.TypeCode(name = "TIME", type = "java.sql.Time"),
    @GenerateTypeCodes.TypeCode(name = "TIMESTAMP", type = "java.sql.Timestamp"),
    // collection types
    @GenerateTypeCodes.TypeCode(name = "ARRAY", type = "java.lang.Object[]"),
    @GenerateTypeCodes.TypeCode(name = "LIST", type = "java.util.List",
        aliases = {"MULTISET"}),
    @GenerateTypeCodes.TypeCode(name = "MAP", type = "java.util.Map"),
    // pseudo types
    @GenerateTypeCodes.TypeCode(name = "TUPLE", type = "tuple"),
    @GenerateTypeCodes.TypeCode(name = "DICT", type = "dict"),
    @GenerateTypeCodes.TypeCode(name = "NULL", type = "java.lang.Void"),
})
package io.dingodb.expr.runtime;

import io.dingodb.expr.annotations.GenerateTypeCodes;
