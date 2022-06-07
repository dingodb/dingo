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
import io.dingodb.sdk.client.DingoClient;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

@Slf4j
public class JsonFetch extends AbstractParser implements Fetch {

    private static ObjectMapper mapper = new ObjectMapper();

    @Override
    public void fetch(
        String localFile, String separator, boolean state, DingoClient dingoClient, TableDefinition tableDefinition) {
        try {
            List<Object[]> records = new ArrayList<>();
            BufferedReader br = new BufferedReader(new FileReader(localFile));
            String line = br.readLine();
            if (line.charAt(0) == '[') {
                StringBuffer sb = new StringBuffer();
                sb.append(line);
                while ((line = br.readLine()) != null) {
                    sb.append(line);
                }
                List<LinkedHashMap<String, Object>> list =
                    mapper.readValue(sb.toString(), new TypeReference<List<LinkedHashMap<String, Object>>>() {});
                records.addAll(list.stream().map(l -> l.values().toArray()).collect(Collectors.toList()));
            } else {
                records.add(readLine(line).values().toArray());
                while ((line = br.readLine()) != null) {
                    records.add(readLine(line).values().toArray());
                    if (records.size() >= 1000) {
                        this.parse(tableDefinition, records, dingoClient);
                    }
                }
            }
            if (records.size() != 0) {
                this.parse(tableDefinition, records, dingoClient);
            }
        } catch (IOException e) {
            log.error("Error reading file:{}", localFile, e);
        }
    }

    @Override
    public void parse(TableDefinition tableDefinition, List<Object[]> records, DingoClient dingoClient) {
        super.parse(tableDefinition, records, dingoClient);
    }

    @Override
    public void fetch(Properties props, String topic, DingoClient dingoClient, TableDefinition tableDefinition) {
    }

    private LinkedHashMap<String, Object> readLine(String line) throws JsonProcessingException {
        return mapper.readValue(line, new TypeReference<LinkedHashMap<String, Object>>() {});
    }
}
