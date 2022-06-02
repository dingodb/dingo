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
import io.dingodb.cli.source.Parser;
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

@Slf4j
public class JsonFetch implements Fetch {

    private static ObjectMapper mapper = new ObjectMapper();

    @Override
    public List<Object[]> fetch(String localFile, String tableName, String separator, boolean state) {
        List<Object[]> records = new ArrayList<>();
        try {
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
                records.add(list.stream().map(LinkedHashMap::values).toArray());
            } else {
                records.add(readLine(line).values().toArray());
                while ((line = br.readLine()) != null) {
                    records.add(readLine(line).values().toArray());
                }
            }
        } catch (IOException e) {
            log.error("Error reading file:{}", localFile, e);
        }
        return records;
    }

    @Override
    public void fetch(Properties props, String topic, Parser parser, DingoClient dingoClient, TableDefinition tableDefinition) {
    }

    private LinkedHashMap<String, Object> readLine(String line) throws JsonProcessingException {
        return mapper.readValue(line, new TypeReference<LinkedHashMap<String, Object>>() {});
    }
}
