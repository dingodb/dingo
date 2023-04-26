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
import io.dingodb.cli.source.impl.AvroConverter;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.type.DingoType;
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
    public void fetch(Properties props, String topic, TableDefinition tableDefinition) {
        deserializer.configure(
            Collections.singletonMap("schema.registry.url", props.getProperty("schema.registry.url")), false);

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
        long totalInsertCnt = 0L;
        List<Object[]> result = new ArrayList<>();
        for (Object[] arr : records) {
            try {
                DingoType schema = tableDefinition.getDingoType();
                Object[] row = (Object[]) schema.convertFrom(arr, AvroConverter.INSTANCE);
                if (!isValidRecord(tableDefinition, row)) {
                    log.warn("Invalid input row will skip it:{}", arr);
                    continue;
                }
                result.add(row);
                totalInsertCnt++;
            } catch (Exception ex) {
                log.warn("Data:{} parsing failed", arr, ex);
            }
        }
        try {
            //dingoClient.insert(tableDefinition.getName().toUpperCase(), result);
        } catch (Exception e) {
            log.error("Error encoding record", e);
            totalInsertCnt = 0;
        }
        return totalInsertCnt;
    }

}
