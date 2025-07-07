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

package io.dingodb.common.parser;

import io.dingodb.common.mysql.parser.MySqlLexer;

public class SQLParserUtils {
    public static Lexer createLexer(String sql, DbType dbType) {
        return createLexer(sql, dbType, new SQLParserFeature[0]);
    }

    public static Lexer createLexer(String sqlString, DbType dbType, SQLParserFeature... features) {
        ByteString sql = ByteString.from(sqlString);

        return new MySqlLexer(sql);
    }
}
