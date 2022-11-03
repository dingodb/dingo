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

package io.dingodb.expr.parser.exception;

import lombok.Getter;

import java.util.List;

public final class ExprSyntaxError extends ExprParseException {
    private static final long serialVersionUID = -729878914565812713L;
    @Getter
    private final List<String> errorMessages;

    /**
     * Exception thrown when there are errors during syntax parsing (by ANTLR4).
     *
     * @param errorMessages the error messages coming from ANTLR4
     */
    public ExprSyntaxError(List<String> errorMessages) {
        super(
            "Dingo expression syntax error:\n" + String.join("\n", errorMessages)
        );
        this.errorMessages = errorMessages;
    }
}
