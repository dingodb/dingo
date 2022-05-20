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

package io.dingodb.cli.parser;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import io.dingodb.cli.parser.impl.DefaultParserFactory;
import io.dingodb.sdk.client.DingoClient;

public class Import {

    @Parameter(names = "--help", help = true, order = 0)
    private boolean help;

    @Parameter(description = "command", order = 1, required = true)
    private String cmd;

    @Parameter(names = "--config", description = "Config file path.", order = 2)
    private String config;

    @Parameter(names = "--record-type", description = "File type")
    private String recordType;

    @Parameter(names = "--file-path", description = "local file path")
    private String localFile;

    @Parameter(names = "--table-name", description = "table name")
    private String tableName;

    @Parameter(names = "--separator", description = "csv separator")
    private String separator;

    @Parameter(names = "--use-header", description = "Does the csv file have headers")
    private boolean state;

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
        switch (cmd.toUpperCase()) {
            case "SOURCE":
                DingoClient dingoClient = new DingoClient(config, tableName.toUpperCase());
                ParserFactory parserFactory = new DefaultParserFactory();
                Parser parser = parserFactory.getParser(recordType.toUpperCase());
                parser.parse(localFile, separator, tableName.toUpperCase(), state, dingoClient);
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + cmd);
        }

    }
}
