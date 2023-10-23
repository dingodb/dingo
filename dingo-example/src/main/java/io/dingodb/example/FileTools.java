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

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import io.dingodb.client.DingoClient;
import io.dingodb.client.common.Key;
import io.dingodb.client.common.Record;
import io.dingodb.client.common.Value;
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.sdk.common.table.Table;
import lombok.extern.slf4j.Slf4j;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.FileWriter;
import java.util.Iterator;

@Slf4j
public class FileTools {

    @Parameter(names = "--help", help = true, order = 0)
    private boolean help;

    @Parameter(description = "[import] or [export]", order = 1, required = true)
    private String cmd;

    @Parameter(names = "--config", description = "Config file path.", order = 2, required = true)
    private String config;

    @Parameter(names = "--file", description = "local file path", order = 3, required = true)
    private String localFile;

    @Parameter(names = "--table", description = "table name", order = 4, required = true)
    private String tableName;

    @Parameter(names = "--start", description = "Start primary key value when scanning data, default 1", order = 5)
    private Long start = 1L;

    @Parameter(names = "--end", description = "End primary key value when scanning data, default 1000", order = 6)
    private Long end = 1000L;

    public static void main(String[] args) throws Exception {
        FileTools exec = new FileTools();
        JCommander commander = new JCommander(exec);
        commander.parse(args);
        exec.exec(commander);
    }

    public void exec(JCommander commander) throws Exception {
        if (help) {
            commander.usage();
            return;
        }

        DingoConfiguration.parse(config);
        String coordinatorServerList = Configuration.coordinatorExchangeSvrList();
        DingoClient dingoClient = new DingoClient(coordinatorServerList);

        switch (cmd.toUpperCase()) {
            case "IMPORT":
                Table table = dingoClient.getTableDefinition(tableName);
                JsonFetch fetch = new JsonFetch();
                if (localFile == null) {
                    System.out.println("File-path cannot be empty \n");
                }
                fetch.fetch(localFile, table, dingoClient);
                break;
            case "EXPORT":
                Iterator<Record> iterator = dingoClient.scan(tableName, new Key(Value.get(start)), new Key(Value.get(end)), true, true);
                FileWriter fileWriter = new FileWriter(localFile);
                while (iterator.hasNext()) {
                    JSONObject jsonObject = (JSONObject) new JSONParser().parse(iterator.next().toJson());
                    fileWriter.write((jsonObject).toJSONString() + "\t\n");
                }
                fileWriter.close();
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + cmd.toUpperCase());
        }

        dingoClient.close();
    }

}
