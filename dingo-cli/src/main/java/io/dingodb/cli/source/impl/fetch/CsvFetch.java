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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.google.common.base.Strings;
import io.dingodb.cli.source.Fetch;
import io.dingodb.cli.source.impl.AbstractParser;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.sdk.client.DingoClient;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

@Slf4j
public class CsvFetch extends AbstractParser implements Fetch {

    private ObjectMapper mapper = new CsvMapper();
    private CsvSchema schema = CsvSchema.builder()
        .setAllowComments(true)
        .build();

    @Override
    public void fetch(
        String localFile, String separator, boolean state, DingoClient dingoClient, TableDefinition tableDefinition) {
        try {
            List<Object[]> records = new ArrayList<>();
            separator = Optional.of(separator.trim()).orElse(",");
            char separatorChar;
            if (separator.length() > 1 && separator.startsWith("\\") ) {
                separatorChar = separator.charAt(1);
            } else {
                separatorChar = separator.charAt(0);
            }
            BufferedReader br = new BufferedReader(new FileReader(localFile));
            String line = "";
            if (state) {
                br.readLine();
            }
            while ((line = br.readLine()) != null) {
                if (Strings.isNullOrEmpty(line)) {
                    continue;
                }
                Object[] arr = mapper.readerFor(Object[].class)
                    .with(schema.withColumnSeparator(separatorChar))
                    .readValue(line);
                records.add(arr);
                if (records.size() >= 1000) {
                    this.parse(tableDefinition, records, dingoClient);
                    records.clear();
                }
            }
            if (records.size() != 0) {
                this.parse(tableDefinition, records, dingoClient);
            }
        } catch (Exception e) {
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
}
