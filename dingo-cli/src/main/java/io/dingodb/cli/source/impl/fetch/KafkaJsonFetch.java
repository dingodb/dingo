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

package io.dingodb.cli.source.impl.fetch;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.dingodb.cli.source.Fetch;
import io.dingodb.cli.source.impl.AbstractParser;
import io.dingodb.common.table.TableDefinition;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Properties;

@Slf4j
public class KafkaJsonFetch extends AbstractParser implements Fetch {

    private ObjectMapper mapper = new ObjectMapper();

    @Override
    public void fetch(Properties props, String topic, TableDefinition tableDefinition) {

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));
        try {
            while (true) {
                List<Object[]> result = new ArrayList<>();
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        LinkedHashMap<String, Object> map = mapper.readValue(record.value(),
                            new TypeReference<LinkedHashMap<String, Object>>() {});
                        result.add(map.values().toArray());
                    } catch (JsonProcessingException e) {
                        log.error("The data:{} is not in JSON format, and parsing fails", record.value());
                    }
                    if (result.size() >= 1000) {
                        this.parse(tableDefinition, result);
                        result.clear();
                    }
                }
                if (result.size() != 0) {
                    this.parse(tableDefinition, result);
                    result.clear();
                }
            }
        } finally {
            consumer.close();
        }
    }

    @Override
    public void fetch(String localFile, String separatorOrPattern, boolean state, TableDefinition tableDefinition) {
    }

    @Override
    public long parse(TableDefinition tableDefinition, List<Object[]> records) {
        return super.parse(tableDefinition, records);
    }
}
