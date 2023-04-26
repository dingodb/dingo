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
        String localFile,
        String separator,
        boolean state,
        TableDefinition tableDefinition) {
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
            long totalReadCnt = 0L;
            long totalWriteCnt = 0L;
            while ((line = br.readLine()) != null) {
                if (Strings.isNullOrEmpty(line)) {
                    continue;
                }
                Object[] arr = mapper.readerFor(Object[].class)
                    .with(schema.withColumnSeparator(separatorChar))
                    .readValue(line);
                records.add(arr);
                totalReadCnt++;
                if (records.size() >= 1000) {
                    totalWriteCnt += this.parse(tableDefinition, records);
                    records.clear();
                }
            }
            if (records.size() != 0) {
                totalWriteCnt += this.parse(tableDefinition, records);
            }
            System.out.println("The total read count from File is:" + totalReadCnt
                + ", real write count:" + totalWriteCnt);
        } catch (Exception ex) {
            log.error("Error reading file:{}", localFile, ex);
        }
    }

    @Override
    public void fetch(Properties props, String topic, TableDefinition tableDefinition) {
    }

    @Override
    public long parse(TableDefinition tableDefinition, List<Object[]> records) {
        return super.parse(tableDefinition, records);
    }
}
