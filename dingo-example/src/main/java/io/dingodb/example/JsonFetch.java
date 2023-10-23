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

package io.dingodb.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.dingodb.client.DingoClient;
import io.dingodb.client.common.KeyValueCodec;
import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.DingoTypeFactory;
import io.dingodb.sdk.common.table.Column;
import io.dingodb.sdk.common.table.Table;
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

@Slf4j
public class JsonFetch extends AbstractParser {

    private static ObjectMapper mapper = new ObjectMapper();

    public void fetch(String localFile, Table table, DingoClient dingoClient) {
        try {
            long totalReadCnt = 0L;
            long totalWriteCnt = 0L;
            JSONParser parser = new JSONParser();
            Object parserJsonObj = parser.parse(new FileReader(localFile));

            if (parserJsonObj instanceof JSONObject) {
                String inputStr = parserJsonObj.toString();
                totalReadCnt++;
                List<Object[]> records = new ArrayList<>();
                records.add(parseSingleRow(inputStr, table));
                totalWriteCnt += this.parse(dingoClient, table, records);
            } else if (parserJsonObj instanceof JSONArray) {
                JSONArray jsonArray = (JSONArray) (parserJsonObj);
                for (Object o : jsonArray) {
                    JSONObject jsonObject = (JSONObject) o;
                    totalReadCnt++;
                    List<Object[]> records = new ArrayList<>();
                    records.add(parseSingleRow(jsonObject.toString(), table));
                    totalWriteCnt += this.parse(dingoClient, table, records);
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
            doParseJsonRecord(localFile, table, dingoClient);
        } catch (IOException exception) {
            log.error("IO Exception:{}", exception.toString(), exception);
        }
    }

    private void doParseJsonRecord(final String localFile,
                                   final Table table, DingoClient dingoClient) throws IOException {
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

            records.add(parseSingleRow(line, table));
            totalReadCnt++;
            if (records.size() >= 1000) {
                totalWriteCnt += this.parse(dingoClient, table, records);
                records.clear();
            }
        }
        if (records.size() != 0) {
            totalWriteCnt += this.parse(dingoClient, table, records);
        }
        System.out.println("LineMode=>The total read count from File is:" + totalReadCnt
            + ", real write count:" + totalWriteCnt);
    }

    public long parse(DingoClient dingoClient, Table table, List<Object[]> records) {
        long totalInsertCnt = 0L;
        for (Object[] arr : records) {
            try {
                DingoType schema = getDingoType(table.getColumns());
                Object[] row = (Object[]) schema.parse(arr);
                if (!isValidRecord(table, row)) {
                    log.warn("Invalid input row will skip it:{}", arr);
                    continue;
                }
                boolean upsertIndex = dingoClient.upsertIndex(table.getName(), row);
                totalInsertCnt = upsertIndex ? totalInsertCnt + 1 : totalInsertCnt;
            } catch (Exception ex) {
                log.warn("Data:{} parsing failed", arr, ex);
            }
        }
        return totalInsertCnt;
    }

    private static DingoType getDingoType(List<Column> columns) {
        return DingoTypeFactory.tuple(columns.stream().map(JsonFetch::getDingoType).toArray(DingoType[]::new));
    }

    private static DingoType getDingoType(Column column) {
        return DingoTypeFactory.fromName(column.getType(), column.getElementType(), column.isNullable());
    }

    protected boolean isValidRecord(Table table, Object[] columnValues) {
        if (table == null
            || columnValues == null
            || columnValues.length != table.getColumns().size()) {
            log.error("Invalid record expectedCnt:{}, realCnt:{}",
                table == null ? 0 : table.getColumns().size(),
                columnValues == null ? 0 : columnValues.length);
            return false;
        }

        boolean isValid = true;
        for (int i = 0; i < columnValues.length; i++) {
            Column column = table.getColumns().get(i);
            if (!column.isNullable() && columnValues[i] == null) {
                isValid = false;
                log.error("Column:{} is not null, but current value is null", column.getName());
                break;
            }
        }
        return isValid;
    }

    private Object[] parseSingleRow(String line, Table table) throws JsonProcessingException {
        LinkedHashMap<String, Object> resultOfLinkedMap = mapper.readValue(
            line, new TypeReference<LinkedHashMap<String, Object>>() {}
        );

        int columnSize = resultOfLinkedMap.keySet().size();
        List<Object> resultList = new ArrayList<>(columnSize);

        /**
         * to avoid column name is upper case or low case.
         */
        for (Column column: table.getColumns()) {
            for (String columnName: resultOfLinkedMap.keySet()) {
                if (columnName.compareToIgnoreCase(column.getName()) == 0) {
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
