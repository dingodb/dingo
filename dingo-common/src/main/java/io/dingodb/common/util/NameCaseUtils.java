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

package io.dingodb.common.util;

import io.dingodb.common.config.DingoConfiguration;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class NameCaseUtils {

    private NameCaseUtils() {
    }

    public static String convertSql(String sql) {
        Integer caseValue = DingoConfiguration.lowerCaseTableNames();
        if (caseValue == 0) {
            return sql;
        } else if (caseValue == 1) {
            return convert(sql, caseValue);
        } else if (caseValue == 2) {
            return convert(sql, caseValue);
        }
        return sql;
    }

    /**
     * Convert schema and table names in SQL statements to lowercase or uppercase.
     * @param sql SQL statement
     * @return The converted SQL statement
     */
    public static String convert(String sql, int caseValue) {
        String pattern = "(?i)(FROM|JOIN|INSERT INTO|UPDATE)\\s+([\\w.]+)";
        Pattern r = Pattern.compile(pattern);
        Matcher m = r.matcher(sql);

        StringBuilder sb = new StringBuilder();
        while (m.find()) {
            String keyword = m.group(1);
            String identifier = m.group(2);
            String identifierConvert = caseValue == 1 ? identifier.toLowerCase() : identifier;
            m.appendReplacement(sb, keyword + " " + identifierConvert);
        }
        m.appendTail(sb);
        return sb.toString();
    }

    public static String convertName(String name) {
        Integer caseValue = DingoConfiguration.lowerCaseTableNames();
        return convertName(name, caseValue);
    }

    public static String convertName(String name, int caseValue) {
        if (name == null) {
            return null;
        }
        if (caseValue == 0 || caseValue == 2) {
            return name;
        } else if (caseValue == 1) {
            return name.toLowerCase();
        } else {
            return name;
        }
    }

    public static boolean caseSensitive() {
        return DingoConfiguration.lowerCaseTableNames() == 0;
    }

}
