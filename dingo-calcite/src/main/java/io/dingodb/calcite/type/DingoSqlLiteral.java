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

package io.dingodb.calcite.type;

import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.NlsString;
import org.checkerframework.checker.nullness.qual.Nullable;

@Slf4j
public final class DingoSqlLiteral {

    private DingoSqlLiteral() {
    }

    public static DingoIntervalLiteral createInterval(
        int sign,
        String intervalStr,
        SqlIntervalQualifier intervalQualifier,
        SqlParserPos pos) {
        return new DingoIntervalLiteral(
            sign,
            intervalStr,
            intervalQualifier,
            intervalQualifier.typeName(),
            pos);
    }

    public static SqlCharStringLiteral createCharString(
        String s,
        @Nullable String charSet,
        SqlParserPos pos) {
        //if (s != null) {
        //    try {
        //        if (s.contains("\\'")) {
        //            s = s.replace("\\'", "'");
        //        }
        //    } catch (Exception e) {
        //        LogUtils.warn(log, e.getMessage(), e);
        //    }
        //}
        NlsString slit = new NlsString(s, charSet, null);
        return new DingoSqlCharStringLiteral(slit, pos);
    }
}
