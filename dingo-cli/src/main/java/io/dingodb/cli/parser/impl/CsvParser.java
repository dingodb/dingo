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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import io.dingodb.cli.parser.Parser;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.table.TupleSchema;
import io.dingodb.sdk.client.DingoClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class CsvParser implements Parser {
    private static final Logger logger = LoggerFactory.getLogger(CsvParser.class);
    private ObjectMapper mapper = new CsvMapper();
    private CsvSchema schema = CsvSchema.builder()
        .setAllowComments(true)
        .build();

    @Override
    public void parse(String localFile, String tableName, DingoClient dingoClient) {
        throw new RuntimeException("Enter wrong parse method");
    }

    @Override
    public void parse(String localFile, String separator, String tableName, boolean state, DingoClient dingoClient) {
        try {
            separator = Optional.of(separator.trim()).orElse(",");
            char separatorChar;
            if (separator.length() > 1 && separator.startsWith("\\") ) {
                separatorChar = separator.charAt(1);
            } else {
                separatorChar = separator.charAt(0);
            }
            TableDefinition tableDefinition = dingoClient.getMetaClient().getTableDefinition(tableName);
            if (tableDefinition == null) {
                System.out.printf("Table:%s not found \n", tableName);
                System.exit(1);
            }
            BufferedReader br = new BufferedReader(new FileReader(localFile));
            String line = "";
            if (state) {
                br.readLine();
            }
            List<Object[]> records = new ArrayList<>();
            while ((line = br.readLine()) != null) {
                String[] arr = mapper.readerFor(String[].class)
                    .with(schema.withColumnSeparator(separatorChar))
                    .readValue(line);
                if (arr.length == tableDefinition.getColumns().size()) {
                    try {
                        TupleSchema schema = tableDefinition.getTupleSchema();
                        Object[] record = schema.parse(arr);
                        records.add(record);
                    } catch (Exception e) {
                        logger.warn("Data:{} parsing failed", line);
                    }
                } else {
                    logger.warn("The current data is missing a field value, skip it:{} ", line);
                }
            }
            dingoClient.insert(records);

        } catch (Exception ex) {
            logger.error("Error parsing csv message", ex);
        }
    }
}
