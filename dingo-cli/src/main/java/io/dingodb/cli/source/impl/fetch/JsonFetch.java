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
import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.common.table.TableDefinition;
import lombok.extern.slf4j.Slf4j;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Properties;

@Slf4j
public class JsonFetch extends AbstractParser implements Fetch {

    private static ObjectMapper mapper = new ObjectMapper();

    @Override
    public void fetch(
            String localFile,
            String separator,
            boolean state,
            TableDefinition tableDefinition) {
        try {
            long totalReadCnt = 0L;
            long totalWriteCnt = 0L;
            JSONParser parser = new JSONParser();
            Object parserJsonObj = parser.parse(new FileReader(localFile));

            if (parserJsonObj instanceof JSONObject) {
                String inputStr = parserJsonObj.toString();
                totalReadCnt++;
                List<Object[]> records = new ArrayList<>();
                records.add(parseSingleRow(inputStr, tableDefinition));
                totalWriteCnt += this.parse(tableDefinition, records);
            } else if (parserJsonObj instanceof JSONArray) {
                JSONArray jsonArray = (JSONArray) (parserJsonObj);
                for (Object o : jsonArray) {
                    JSONObject jsonObject = (JSONObject) o;
                    totalReadCnt++;
                    List<Object[]> records = new ArrayList<>();
                    records.add(parseSingleRow(jsonObject.toString(), tableDefinition));
                    totalWriteCnt += this.parse(tableDefinition, records);
                }
            }
            System.out.println("FileMode=>The total read count from File is:" + totalReadCnt
                + ", real write count:" + totalWriteCnt);
            return;
        } catch (IOException fileException) {
            log.error("Error file:{} catch IO Exception:{}",
                localFile, fileException.toString(), fileException);
        } catch (ParseException parseException) {
            log.error("Error file:{} catch parser exception:{}",
                localFile, parseException.toString(), parseException);
        }

        try {
            doParseJsonRecord(localFile, tableDefinition);
        } catch (IOException exception) {
            log.error("IO Exception:{}", exception.toString(), exception);
        }
    }

    @Override
    public void fetch(Properties props,
        String topic,
        TableDefinition tableDefinition) {
    }

    private void doParseJsonRecord(final String localFile,
                                   final TableDefinition tableDefinition) throws IOException {
        long totalReadCnt = 0L;
        long totalWriteCnt = 0L;
        String line = "";
        List<Object[]> records = new ArrayList<>();
        BufferedReader br = new BufferedReader(new FileReader(localFile));

        while ((line = br.readLine()) != null) {
            line = line.trim();
            if (!isValidRecord(line)) {
                continue;
            }
            // skip the header line '[{"id":1,"name":"dingo"},{"id":2,"name":"dingo"}]'
            if (line.charAt(0) == '[') {
                line = line.substring(1);
            }
            if (line.charAt(line.length() - 1) == ']') {
                line = line.substring(0, line.length() - 1);
            }

            records.add(parseSingleRow(line, tableDefinition));
            totalReadCnt++;
            if (records.size() >= 1000) {
                totalWriteCnt += this.parse(tableDefinition, records);
                records.clear();
            }
        }
        if (records.size() != 0) {
            totalWriteCnt += this.parse(tableDefinition, records);
        }
        System.out.println("LineMode=>The total read count from File is:" + totalReadCnt
            + ", real write count:" + totalWriteCnt);
    }

    @Override
    public long parse(TableDefinition tableDefinition, List<Object[]> records) {
        return super.parse(tableDefinition, records);
    }

    private Object[] parseSingleRow(String line, TableDefinition definition) throws JsonProcessingException {
        LinkedHashMap<String, Object> resultOfLinkedMap = mapper.readValue(
            line, new TypeReference<LinkedHashMap<String, Object>>() {}
        );

        int columnSize = resultOfLinkedMap.keySet().size();
        List<Object> resultList = new ArrayList<>(columnSize);

        /**
         * to avoid column name is upper case or low case.
         */
        for (ColumnDefinition columnDefinition: definition.getColumns()) {
            for (String columnName: resultOfLinkedMap.keySet()) {
                if (columnName.compareToIgnoreCase(columnDefinition.getName()) == 0) {
                    resultList.add(resultOfLinkedMap.get(columnName));
                }
            }
        }
        return resultList.toArray();
    }

    private boolean isValidRecord(final String inputLine) {
        if (inputLine == null || inputLine.isEmpty()) {
            return false;
        }

        String line = inputLine.trim();
        if (line.length() == 1 && line.charAt(0) == '[') {
            return false;
        }

        if (line.length() == 1 && line.charAt(0) == ']') {
            return false;
        }
        return true;
    }
}
