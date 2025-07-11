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

package io.dingodb.driver.mysql;

import io.dingodb.common.mysql.DingoErrUtil;
import io.dingodb.common.mysql.parser.MySqlLexer;
import io.dingodb.common.parser.ByteString;
import io.dingodb.common.parser.Token;

import java.util.ArrayList;
import java.util.List;

public class MultiStatementSplitter {
    private final ByteString raw;

    private final MySqlLexer lexer;

    public MultiStatementSplitter(ByteString raw) {
        this.raw = raw;
        this.lexer = new MySqlLexer(raw);
    }

    public List<ByteString> split() {
        /*
         * Find and cut on semi ';'
         * Note that following elements must be skipped ignoring any ';' inside it
         * 1. Inline comments (-- foo or #foo)
         * 2. Multi-line comments
         * 3. String literals ("foo" or 'foo')
         */
        List<ByteString> statements = new ArrayList<>();

        try {
            int startPos = 0;
            boolean hasToken = false;

            do {
                lexer.nextToken();

                if (lexer.token() == Token.PROCEDURE || lexer.token() == Token.TRIGGER
                    || lexer.token() == Token.FUNCTION) {
                    List<ByteString> origin = new ArrayList<>();
                    origin.add(raw);
                    return origin;
                }

                if (lexer.token() == Token.SEMI) {
                    if (hasToken) {
                        statements.add(raw.slice(startPos, lexer.pos()));
                    }
                    startPos = lexer.pos();
                    hasToken = false;
                } else if (lexer.token() != Token.EOF) {
                    hasToken = true;
                }
            } while (lexer.token() != Token.EOF);

            if (hasToken) {
                statements.add(raw.slice(startPos));
            }
        } catch (Exception e) {
            throw DingoErrUtil.newStdErr("failed to split sql");
        }

        return statements;
    }
}
