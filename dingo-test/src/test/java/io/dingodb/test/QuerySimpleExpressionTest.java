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

package io.dingodb.test;

import io.dingodb.expr.test.ExprTestUtils;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

import static org.junit.jupiter.params.provider.Arguments.arguments;

@Slf4j
public class QuerySimpleExpressionTest {
    private static SqlHelper sqlHelper;

    @BeforeAll
    public static void setupAll() throws Exception {
        sqlHelper = new SqlHelper();
    }

    @AfterAll
    public static void cleanUpAll() throws Exception {
        sqlHelper.cleanUp();
    }

    @Nonnull
    public static Stream<Arguments> getParameters() {
        return Stream.of(
            arguments("1 + 1", 2),
            arguments("1 + 100000000.2", BigDecimal.valueOf(100000001.2)),
            arguments("1 + 100.1", BigDecimal.valueOf(101.1)),
            arguments("'hello'", "hello"),
            arguments("'abc' || null", null),
            arguments("round(123, -2)", 100),
            // string
            arguments("'AA' || 'BB' || 'CC'", "AABBCC"),
            arguments("concat(concat('AA', 'BB'), 'CC')", "AABBCC"),
            arguments("left('ABC', 2)", "AB"),
            arguments("left('ABCDE', 10)", "ABCDE"),
            arguments("left('ABCDE', -3)", ""),
            arguments("left('ABC-DEF', 3)", "ABC"),
            arguments("right('ABC', 1)", "C"),
            arguments("right('ABC', 1.5)", "BC"),
            arguments("ltrim(' AAA ')", "AAA "),
            arguments("rtrim(' AAA ')", " AAA"),
            arguments("'a'||rtrim(' ')||'b'", "ab"),
            arguments("locate('\\c', 'ab\\cde')", 3),
            arguments("locate('C', 'ABCd')", 3),
            arguments("locate('abc', '')", 0),
            arguments("locate('', '')", 1),
            arguments("mid('ABC', 1, 2)", "AB"),
            arguments("mid('ABC', 2, 3)", "BC"),
            arguments("mid('ABCDEFG', -5, 3)", "CDE"),
            arguments("mid('ABCDEFG', 1, -5)", ""),
            arguments("mid('ABCDEFG', 2)", "BCDEFG"),
            arguments("mid('ABCDEFG', 2.5, 3)", "CDE"),
            arguments("mid('ABCDEFG', 2, 3.5)", "BCDE"),
            arguments("repeat('ABC', 3)", "ABCABCABC"),
            arguments("repeat('ABC', 2.1)", "ABCABC"),
            arguments("replace('MongoDB', 'Mongo', 'Dingo')", "DingoDB"),
            arguments("replace(null, 'a', 'b')", null),
            arguments("replace('MongoDB', null, 'b')", null),
            arguments("replace(null, null, 'ftest')", null),
            arguments("reverse('ABC')", "CBA"),
            arguments("substring('DingoDatabase', 1, 5)", "Dingo"),
            arguments("substring('DingoDatabase', 1, 100)", "DingoDatabase"),
            arguments("substring('DingoDatabase', 2, 2.5)", "ing"),
            arguments("substring('DingoDatabase', 2, -3)", ""),
            arguments("substring('DingoDatabase', -4, 4)", "base"),
            arguments("substring('abcde', 1, 6)", "abcde"),
            arguments("substring('', 1, 3)", ""),
            arguments("substring(null, 1, 1)", null),
            arguments("trim(' AAAAA  ')", "AAAAA"),
            arguments("trim(BOTH 'A' from 'ABBA')", "BB"),
            arguments("trim('  ')", ""),
            arguments("trim(' A ')", "A"),
            arguments("trim(123 from 1234123)", "4"),
            arguments("trim(LEADING 'A' from 'ABBA')", "BBA"),
            arguments("trim(TRAILING 'A' from 'ABBA')", "ABB"),
            arguments("upper('aaa')", "AAA"),
            arguments("ucase('aaa')", "AAA"),
            arguments("lower('aaA')", "aaa"),
            arguments("lcase('Aaa')", "aaa"),
            // date & time
            arguments("date_format('2022/7/2')", "2022-07-02"),
            arguments("date_format('', '%Y-%m-%d')", null),
            arguments("time_format('235959')", "23:59:59"),
            arguments("timestamp_format('2022/07/22 12:00:00')", "2022-07-22 12:00:00"),
            arguments("datediff('', '')", null)
        );
    }

    @ParameterizedTest
    @MethodSource("getParameters")
    public void test(String sql, Object value) throws SQLException {
        Object result = sqlHelper.querySingleValue("select " + sql);
        ExprTestUtils.assertEquals(result, value);
    }
}
