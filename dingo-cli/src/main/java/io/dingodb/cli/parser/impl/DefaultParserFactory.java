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

package io.dingodb.cli.parser.impl;

import io.dingodb.cli.parser.Parser;
import io.dingodb.cli.parser.ParserFactory;

import java.util.HashMap;
import java.util.Map;

public class DefaultParserFactory implements ParserFactory {

    private Map<String, Parser> parsers;

    /**
     * Init DefaultParserFactory.
     */
    public DefaultParserFactory() {
        parsers = new HashMap<>();
        parsers.put("CSV", new CsvParser());
        parsers.put("JSON", new JsonParser());
    }

    @Override
    public Parser getParser(String format) {
        return parsers.get(format);
    }
}
