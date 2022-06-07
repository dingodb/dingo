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

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.dingodb.cli.source.Fetch;
import io.dingodb.cli.source.impl.AbstractParser;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.sdk.client.DingoClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

@Slf4j
public class KafkaAvroFetch extends AbstractParser implements Fetch {

    private KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer();

    @Override
    public void fetch(Properties props, String topic, DingoClient dingoClient, TableDefinition tableDefinition) {
        deserializer.configure(Collections.singletonMap("schema.registry.url", props.getProperty("schema.registry.url")), false);

        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));
        try {
            while (true) {
                List<Object[]> result = new ArrayList<>();
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, byte[]> record : records) {
                    try {
                        GenericData.Record genericRecord =
                            (GenericData.Record) deserializer.deserialize(topic, record.value());
                        Object[] data = genericRecord.getSchema().getFields()
                            .stream()
                            .map(field -> genericRecord.get(field.name()))
                            .toArray();
                        result.add(data);
                    } catch (Exception e) {
                        log.error("Avro deserialization failed ", e);
                    }
                    if (result.size() >= 1000) {
                        this.parse(tableDefinition, result, dingoClient);
                        result.clear();
                    }
                }
                if (result.size() != 0) {
                    this.parse(tableDefinition, result, dingoClient);
                    result.clear();
                }
            }
        } finally {
            consumer.close();
        }
    }

    @Override
    public void parse(TableDefinition tableDefinition, List<Object[]> records, DingoClient dingoClient) {
        super.parse(tableDefinition, records, dingoClient);
    }

    @Override
    public void fetch(String localFile, String separatorOrPattern, boolean state,
                      DingoClient dingoClient, TableDefinition tableDefinition) {

    }
}
