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
import io.dingodb.expr.core.TypeCode;
import io.dingodb.expr.parser.Expr;
import io.dingodb.expr.parser.exception.ExprParseException;
import io.dingodb.expr.parser.parser.DingoExprCompiler;
import io.dingodb.expr.runtime.CompileContext;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class TestExprCodeVisitor {
    private static DingoExprCompiler compiler;

    @BeforeAll
    public static void setupAll() {
        compiler = new DingoExprCompiler();
    }

    public static @NonNull Stream<Arguments> getParametersConst() {
        return Stream.of(
            arguments("1", TypeCode.INT, "1101"),
            arguments("-1", TypeCode.INT, "2101"),
            arguments("150", TypeCode.INT, "119601"),
            arguments("-150", TypeCode.INT, "219601"),
            arguments("true", TypeCode.BOOL, "13"),
            arguments("false", TypeCode.BOOL, "23"),
            arguments("1 + 1", TypeCode.INT, "110111018301"),
            arguments("2 + 3", TypeCode.INT, "110211038301"),
            arguments("3 + 4 * 6", TypeCode.INT, "11031104110685018301"),
            arguments("5 + 6 = 11", TypeCode.BOOL, "110511068301110B9101"),
            arguments("7 + 8 > 14 && 6 < 5", TypeCode.BOOL, "110711088301110E930111061105950152"),
            arguments("long(21)", TypeCode.LONG, "1115F021")
        );
    }

    public static @NonNull Stream<Arguments> getParametersVar() {
        return Stream.of(
            arguments("_[0]", TypeCode.INT, "3100"),
            arguments("_[2]", TypeCode.DOUBLE, "3502"),
            arguments("3 + _[0]", TypeCode.INT, "110331008301")
        );
    }

    private static void doTest(
        String exprString,
        Integer targetTypeCode,
        String targetCode,
        CompileContext ctx
    ) throws ExprParseException {
        Expr expr = compiler.parse(exprString);
        ExprCodeVisitor visitor = new ExprCodeVisitor(ctx, null);
        ExprCodeType ect = expr.accept(visitor);
        assertThat(ect.getType()).isEqualTo(targetTypeCode);
        assertThat(ect.getCode()).isEqualTo(CodecUtils.hexStringToBytes(targetCode));
    }

    @ParameterizedTest
    @MethodSource("getParametersConst")
    public void testConst(
        String exprString,
        Integer targetTypeCode,
        String targetCode
    ) throws ExprParseException {
        doTest(exprString, targetTypeCode, targetCode, null);
    }

    @ParameterizedTest
    @MethodSource("getParametersVar")
    public void testVar(
        String exprString,
        Integer targetTypeCode,
        String targetCode
    ) throws ExprParseException {
        DingoType tupleType = DingoTypeFactory.tuple("INT", "STRING", "DOUBLE");
        SqlExprCompileContext ctx = new SqlExprCompileContext(tupleType, null, null);
        doTest(exprString, targetTypeCode, targetCode, ctx);
    }
}
