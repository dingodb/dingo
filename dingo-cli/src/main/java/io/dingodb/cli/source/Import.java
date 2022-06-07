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

package io.dingodb.cli.source;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import io.dingodb.cli.source.impl.DefaultFactory;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.sdk.client.DingoClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

@Slf4j
public class Import {

    @Parameter(names = "--help", help = true, order = 0)
    private boolean help;

    @Parameter(description = "command", order = 1, required = true)
    private String cmd;

    @Parameter(names = "--config", description = "Config file path.", order = 2)
    private String config;

    @Parameter(names = "--record-type", description = "Data type, support json/csv/avro/kafka_json", required = true)
    private String recordType;

    @Parameter(names = "--file-path", description = "local file path")
    private String localFile;

    @Parameter(names = "--table-name", description = "table name", required = true)
    private String tableName;

    @Parameter(names = "--separator", description = "csv separator")
    private String separator;

    @Parameter(names = "--use-header", description = "Does the csv file have headers")
    private boolean state;

    @Parameter(names = "--bootstrap-servers", description = "Kafka server address")
    private String bootstrapServers;

    @Parameter(names = "--group-id", description = "Kafka consumer group id")
    private String groupId;

    @Parameter(names = "--topic", description = "Kafka topic name")
    private String topic;

    @Parameter(names = "--schema-registry-url", description = "schema registry url, eg: http://ip:8081")
    private String schemaRegistry;

    @Parameter(names = "--offset-reset", description = "Auto offset reset, Default [latest], optional [earliest]")
    private String offsetReset;

    public static void main(String[] args) throws Exception {
        Import anImport = new Import();
        JCommander commander = new JCommander(anImport);
        commander.parse(args);
        anImport.exec(commander);
    }

    public void exec(JCommander commander) throws Exception {
        if (help) {
            commander.usage();
            return;
        }
        DingoClient dingoClient = new DingoClient(config, tableName.toUpperCase());
        TableDefinition tableDefinition = dingoClient.getMetaClient().getTableDefinition(tableName.toUpperCase());
        Factory factory = new DefaultFactory();
        Fetch fetch = factory.getFetch(recordType.toUpperCase());
        switch (cmd.toUpperCase()) {
            case "LOCAL":
                if (localFile == null) {
                    System.out.println("File-path cannot be empty \n");
                }
                fetch.fetch(localFile, separator, state, dingoClient, tableDefinition);
                break;
            case "KAFKA":
                Properties props = buildProp();
                if (recordType.equalsIgnoreCase("AVRO")) {
                    if (schemaRegistry == null) {
                        System.out.println("schema-registry-url cannot be empty \n");
                    }
                    props.put("schema.registry.url", schemaRegistry);
                    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                        "org.apache.kafka.common.serialization.ByteArrayDeserializer");
                }
                fetch.fetch(props, topic, dingoClient, tableDefinition);
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + cmd);
        }
    }

    private Properties buildProp() {
        if (bootstrapServers == null || groupId == null) {
            System.out.println("Parameter bootstrap-servers/group-id cannot be empty \n");
        }
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetReset == null ? "latest" : offsetReset);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }
}
