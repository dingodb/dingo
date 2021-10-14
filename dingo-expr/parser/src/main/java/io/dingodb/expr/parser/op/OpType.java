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

package io.dingodb.expr.parser.op;

import lombok.Getter;

public enum OpType {
    INDEX("[]", Category.BINARY, 0),
    FUN("()", Category.UNARY, 0),
    POS("+", Category.UNARY, 1),
    NEG("-", Category.UNARY, 1),
    ADD(" + ", Category.BINARY, 3),
    SUB(" - ", Category.BINARY, 3),
    MUL("*", Category.BINARY, 2),
    DIV("/", Category.BINARY, 2),
    LT(" < ", Category.BINARY, 4),
    LE(" <= ", Category.BINARY, 4),
    EQ(" == ", Category.BINARY, 4),
    GT(" > ", Category.BINARY, 4),
    GE(" >= ", Category.BINARY, 4),
    NE(" != ", Category.BINARY, 4),
    AND(" && ", Category.BINARY, 7),
    OR(" || ", Category.BINARY, 8),
    NOT("!", Category.UNARY, 6),
    STARTS_WITH(" startsWith ", Category.BINARY, 5),
    ENDS_WITH(" endsWith ", Category.BINARY, 5),
    CONTAINS(" contains ", Category.BINARY, 5),
    MATCHES(" matches ", Category.BINARY, 5);

    @Getter
    private final String symbol;
    @Getter
    private final Category category;
    @Getter
    private final int precedence;

    OpType(String symbol, Category category, int precedence) {
        this.symbol = symbol;
        this.category = category;
        this.precedence = precedence;
    }

    public enum Category {
        UNARY,
        BINARY
    }
}
