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

package io.dingodb.calcite;

import org.apache.calcite.server.DdlExecutor;
import org.apache.calcite.sql.parser.SqlAbstractParserImpl;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.parser.dingo.DingoSqlParserImpl;

import java.io.Reader;

@SuppressWarnings("unused")
public class DingoDdlParserFactory implements SqlParserImplFactory {
    public static final SqlParserImplFactory INSTANCE = new DingoDdlParserFactory();

    private DingoDdlParserFactory() {
    }

    @Override
    public SqlAbstractParserImpl getParser(Reader stream) {
        return DingoSqlParserImpl.FACTORY.getParser(stream);
    }

    @Override
    public DdlExecutor getDdlExecutor() {
        return DingoDdlExecutor.INSTANCE;
    }
}
