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

package io.dingodb.exec.expr;

import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.DingoTypeFactory;
import io.dingodb.exec.utils.CodecUtils;
import io.dingodb.expr.parser.Expr;
import io.dingodb.expr.parser.ExprCompiler;
import io.dingodb.expr.parser.exception.ExprParseException;
import io.dingodb.expr.parser.parser.DingoExprCompiler;
import io.dingodb.expr.runtime.CompileContext;
import io.dingodb.expr.runtime.eval.Eval;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayOutputStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class TestEvalSerializer {
    private static DingoExprCompiler parser;

    @BeforeAll
    public static void setupAll() {
        parser = new DingoExprCompiler();
    }

    public static @NonNull Stream<Arguments> getParametersConst() {
        return Stream.of(
            arguments("1", "1101"),
            arguments("-1", "2101"),
            arguments("150", "119601"),
            arguments("-150", "219601"),
            arguments("true", "13"),
            arguments("false", "23"),
            arguments("'abc'", "1703616263"),
            arguments("1 + 1", "110111018301"),
            arguments("2 + 3", "110211038301"),
            arguments("3 + 4 * 6", "11031104110685018301"),
            arguments("5 + 6 = 11", "110511068301110B9101"),
            arguments("7 + 8 > 14 && 6 < 5", "110711088301110E930111061105950152"),
            arguments("long(21)", "1115F021"),
            arguments("'abc' > 'a'", "17036162631701619307"),
            arguments("is_true(1)", "1101A201")
        );
    }

    public static @NonNull Stream<Arguments> getParametersVar() {
        return Stream.of(
            arguments("_[0]", "3100"),
            arguments("_[2]", "3502"),
            arguments("3 + _[0]", "110331008301"),
            arguments("_[1] == ''", "370117009107")
        );
    }

    private static void doTest(
        String exprString,
        String targetCode,
        CompileContext context
    ) throws ExprParseException {
        Expr expr = parser.parse(exprString);
        Eval eval = expr.accept(ExprCompiler.of(context));
        assertThat(eval).isNotNull();
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        assertThat(eval.accept(new EvalSerializer(os))).isTrue();
        assertThat(os.toByteArray()).isEqualTo(CodecUtils.hexStringToBytes(targetCode));
    }

    @ParameterizedTest
    @MethodSource("getParametersConst")
    public void testConst(String exprString, String targetCode) throws ExprParseException {
        doTest(exprString, targetCode, null);
    }

    @ParameterizedTest
    @MethodSource("getParametersVar")
    public void testVar(
        String exprString,
        String targetCode
    ) throws ExprParseException {
        DingoType tupleType = DingoTypeFactory.tuple("INT", "STRING", "DOUBLE");
        SqlExprCompileContext ctx = new SqlExprCompileContext(tupleType, null, null);
        doTest(exprString, targetCode, ctx);
    }
}
