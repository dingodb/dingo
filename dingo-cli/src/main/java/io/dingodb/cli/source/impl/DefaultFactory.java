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

import io.dingodb.cli.source.Factory;
import io.dingodb.cli.source.Fetch;
import io.dingodb.cli.source.impl.fetch.CsvFetch;
import io.dingodb.cli.source.impl.fetch.JsonFetch;
import io.dingodb.cli.source.impl.fetch.KafkaAvroFetch;
import io.dingodb.cli.source.impl.fetch.KafkaJsonFetch;

import java.util.HashMap;
import java.util.Map;

public class DefaultFactory implements Factory {

    private final Map<String, Fetch> fetches;

    public DefaultFactory() {
        fetches = new HashMap<>();
        fetches.put("CSV", new CsvFetch());
        fetches.put("JSON", new JsonFetch());
        fetches.put("AVRO", new KafkaAvroFetch());
        fetches.put("KAFKA_JSON", new KafkaJsonFetch());
    }

    @Override
    public Fetch getFetch(String format) {
        return fetches.get(format);
    }

}
