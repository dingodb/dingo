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

package io.dingodb.cli.source.impl;

import io.dingodb.cli.source.Fetch;
import io.dingodb.cli.source.Parser;
import io.dingodb.cli.source.Factory;
import io.dingodb.cli.source.impl.fetch.CsvFetch;
import io.dingodb.cli.source.impl.fetch.JsonFetch;
import io.dingodb.cli.source.impl.fetch.KafkaAvroFetch;
import io.dingodb.cli.source.impl.fetch.KafkaJsonFetch;
import io.dingodb.cli.source.impl.parser.CsvParser;
import io.dingodb.cli.source.impl.parser.JsonParser;
import io.dingodb.cli.source.impl.parser.KafkaAvroParser;
import io.dingodb.cli.source.impl.parser.KafkaJsonParser;

import java.util.HashMap;
import java.util.Map;

public class DefaultFactory implements Factory {

    private Map<String, Parser> parsers;
    private Map<String, Fetch> fetchMap;

    public DefaultFactory() {
        parsers = new HashMap<>();
        parsers.put("CSV", new CsvParser());
        parsers.put("JSON", new JsonParser());
        parsers.put("AVRO", new KafkaAvroParser());
        parsers.put("KAFKA_JSON", new KafkaJsonParser());
        fetchMap = new HashMap<>();
        fetchMap.put("CSV", new CsvFetch());
        fetchMap.put("JSON", new JsonFetch());
        fetchMap.put("AVRO", new KafkaAvroFetch());
        fetchMap.put("KAFKA_JSON", new KafkaJsonFetch());
    }

    @Override
    public Fetch getFetch(String format) {
        return fetchMap.get(format);
    }

    @Override
    public Parser getParser(String format) {
        return parsers.get(format);
    }
}
