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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.dingodb.cli.parser.Parser;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.table.TupleSchema;
import io.dingodb.sdk.client.DingoClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;

public class JsonParser implements Parser {
    private static final Logger logger = LoggerFactory.getLogger(JsonParser.class);

    private static ObjectMapper mapper = new ObjectMapper();
    private DingoClient dingoClient;
    private TableDefinition tableDefinition;

    @Override
    public void parse(String localFile, String tableName, DingoClient dingoClient) {
        this.dingoClient = dingoClient;
        this.tableDefinition = dingoClient.getMetaClient().getTableDefinition(tableName);
        if (tableDefinition == null) {
            System.out.printf("Table:%s not found \n", tableName);
            System.exit(1);
        }
        try {
            readLocalFile(localFile);
        } catch (Exception ex) {
            logger.error("Error parsing json message", ex);
        }
    }

    private void insert(LinkedHashMap<String, String> record) {
        if (record.values().size() == this.tableDefinition.getColumns().size()) {
            TupleSchema schema = this.tableDefinition.getTupleSchema();
            String[] arr = new String[record.values().size()];
            try {
                Object[] data = schema.parse(record.values().toArray(arr));
                dingoClient.insert(data);
            } catch (Exception e) {
                logger.error("Data:{} parsing failed", Arrays.toString(arr));
            }
        } else {
            logger.warn("The current data is missing a field value, skip it:{}", record.values());
        }
    }

    public void readLocalFile(String localFile) {
        try {
            BufferedReader br = new BufferedReader(new FileReader(localFile));
            String line = br.readLine();
            if (line.charAt(0) == '[') {
                StringBuffer sb = new StringBuffer();
                sb.append(line);
                while ((line = br.readLine()) != null) {
                    sb.append(line);
                }
                List<LinkedHashMap<String, String>> list =
                    mapper.readValue(sb.toString(), new TypeReference<List<LinkedHashMap<String, String>>>() {});
                list.forEach(this::insert);
            } else {
                insertLine(line);
                while ((line = br.readLine()) != null) {
                    insertLine(line);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void insertLine(String line) throws JsonProcessingException {
        LinkedHashMap<String, String> map =
            mapper.readValue(line, new TypeReference<LinkedHashMap<String, String>>() {});
        insert(map);
    }
}
