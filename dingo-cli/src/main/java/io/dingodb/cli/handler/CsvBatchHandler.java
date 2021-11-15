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

package io.dingodb.cli.handler;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.dingodb.calcite.Connections;
import io.dingodb.helix.part.impl.ZkHelixSpectatorPart;
import io.dingodb.meta.helix.HelixMetaService;
import io.dingodb.net.NetService;
import io.dingodb.net.NetServiceProvider;
import io.dingodb.server.ServerConfiguration;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.FileReader;
import java.net.DatagramSocket;
import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.util.ServiceLoader;
import java.util.StringJoiner;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.stream.Collectors;

import static io.dingodb.expr.json.runtime.Parser.JSON;

public class CsvBatchHandler {

    private static final NetService netService = ServiceLoader.load(NetServiceProvider.class).iterator().next().get();

    private static ScheduledThreadPoolExecutor executor;

    public static void handler(
        String table,
        String fileName,
        boolean showSql,
        int batch,
        int parallel
    ) throws Exception {

        executor = new ScheduledThreadPoolExecutor(parallel);
        CSVParser csvParser = CSVParser.parse(new FileReader(fileName), CSVFormat.DEFAULT.withFirstRecordAsHeader());
        List<String> headers = csvParser.getHeaderNames();
        String headersJson = JSON.stringify(headers);
        String insert = String.format("insert into %s(%s)", table, headersJson.substring(1, headersJson.length() - 2))
            .replaceAll("\"", "");
        init();

        int count = 0;
        StringJoiner joiner = new StringJoiner(",", insert + " values ", "");
        for (CSVRecord record : csvParser) {
            joiner.add(valuesSql(headers, record));
            count++;
            if (count >= batch) {
                count = 0;
                String sql = joiner.toString();
                executor.submit(() -> execSql(sql, showSql));
                joiner = new StringJoiner(",", insert + " values ", "");
            }
        }
        String sql = joiner.toString();
        executor.submit(() -> execSql(sql, showSql));

    }

    private static int execSql(String sql, boolean showSql) throws InterruptedException {
        int update;
        if (showSql) {
            System.out.println(sql);
        }
        try (Connection connection = Connections.getConnection();) {
            try (Statement statement = connection.createStatement()) {
                update = statement.executeUpdate(sql);
                System.out.println("Insert success, size: " + update);
                return update;
            } catch (Exception e) {
                e.printStackTrace();
                return 0;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return 0;
        }
    }

    private static void init() throws Exception {
        DatagramSocket datagramSocket = new DatagramSocket();
        netService.listenPort(datagramSocket.getLocalPort());
        ServerConfiguration.instance().port(datagramSocket.getLocalPort());
        datagramSocket.close();
        ZkHelixSpectatorPart helixSpectatorPart = new ZkHelixSpectatorPart(ServerConfiguration.instance());
        helixSpectatorPart.init();
        helixSpectatorPart.start();
        HelixMetaService.instance().init(helixSpectatorPart);
    }

    private static String valuesSql(List<String> headers, CSVRecord record) {
        try {
            String valuesJson = JSON.stringify(headers.stream().map(record::get).collect(Collectors.toList()));
            return String.format("(%s)", valuesJson.substring(1, valuesJson.length() - 2)).replaceAll("\"", "'");
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return null;
        }
    }

}
