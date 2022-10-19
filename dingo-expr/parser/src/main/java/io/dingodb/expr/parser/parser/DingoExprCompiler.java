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

package io.dingodb.expr.parser.parser;

import io.dingodb.expr.parser.DingoExprLexer;
import io.dingodb.expr.parser.DingoExprParser;
import io.dingodb.expr.parser.DingoExprParserVisitor;
import io.dingodb.expr.parser.Expr;
import io.dingodb.expr.parser.exception.DingoExprParseException;
import io.dingodb.expr.parser.exception.ExprSyntaxError;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.antlr.v4.runtime.tree.ParseTree;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;

public final class DingoExprCompiler {
    private static final DingoExprCompiler INS = new DingoExprCompiler();
    private static final DingoExprCompiler INS_DECIMAL = new DingoExprCompiler(true);

    private final DingoExprParserVisitor<Expr> visitor;

    private DingoExprErrorListener errorListener;

    private DingoExprCompiler() {
        this(false);
    }

    private DingoExprCompiler(boolean realAsBigDecimal) {
        visitor = new DingoExprParserVisitorImpl(realAsBigDecimal);
    }

    public static Expr parse(String input) throws DingoExprParseException {
        return parse(input, false);
    }

    /**
     * Parse a given String input into an Expr.
     *
     * @param input            the given String
     * @param realAsBigDecimal whether a real should be parsed into a {@code BigDecimal} or {@code Double}
     * @return the Expr
     * @throws DingoExprParseException if errors occurred in parsing
     */
    public static Expr parse(String input, boolean realAsBigDecimal) throws DingoExprParseException {
        if (realAsBigDecimal) {
            return INS_DECIMAL.parseInternal(input);
        } else {
            return INS.parseInternal(input);
        }
    }

    private @NonNull DingoExprParser getParser(String input) {
        CharStream stream = CharStreams.fromString(input);
        DingoExprLexer lexer = new DingoExprLexer(stream);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        DingoExprParser parser = new DingoExprParser(tokens);
        parser.removeErrorListeners();
        parser.addErrorListener(errorListener);
        return parser;
    }

    private void collectParseError() throws ExprSyntaxError {
        List<String> errorMessages = errorListener.getErrorMessages();
        if (!errorMessages.isEmpty()) {
            throw new ExprSyntaxError(errorMessages);
        }
    }

    private Expr parseInternal(String input) throws DingoExprParseException {
        errorListener = new DingoExprErrorListener();
        DingoExprParser parser = getParser(input);
        ParseTree tree = parser.expr();
        collectParseError();
        try {
            return visitor.visit(tree);
        } catch (ParseCancellationException e) {
            throw new DingoExprParseException(e);
        }
    }
}
