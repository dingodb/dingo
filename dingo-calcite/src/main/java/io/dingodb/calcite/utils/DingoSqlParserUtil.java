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

package io.dingodb.calcite.utils;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.sql.parser.SqlParserUtil;

public class DingoSqlParserUtil {

    public static String parseString(String s) {
        int i = s.indexOf("'"); // start of body
        if (i > 0) {
            s = s.substring(i);
        }
        s =  SqlParserUtil.strip(s, "'", "'", "''", Casing.UNCHANGED);
        s = s.replace("\\'", "'");
        return s;
    }

}
