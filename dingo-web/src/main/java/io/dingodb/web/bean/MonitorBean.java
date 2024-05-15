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

package io.dingodb.web.bean;

import io.dingodb.sdk.service.cluster.ClusterServiceClient;
import io.dingodb.sdk.service.connector.CoordinatorServiceConnector;
import io.dingodb.sdk.service.meta.MetaServiceClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Collectors;

@Slf4j
@Configuration
public class MonitorBean {

    @Bean
    public static ClusterServiceClient clusterServiceClient(@Value("${server.coordinatorExchangeSvrList}") String coordinator) {
        CoordinatorServiceConnector connector = getCoordinatorServiceConnector(coordinator);
        return new ClusterServiceClient(connector);
    }

    @Bean
    public static MetaServiceClient rootMetaServiceClient(@Value("${server.coordinatorExchangeSvrList}") String coordinator) {
        return new MetaServiceClient(coordinator);
    }

    @Bean
    public static Thread logEvent(@Value("${server.monitor.executor.logPath}") String logPath) {
        Thread logThread = new Thread(() -> getLog(logPath));
        Thread eventThread = new Thread(() -> getEvent(logPath));
        logThread.start();
        eventThread.start();
        return eventThread;
    }

    public static void getLog(String logPath) {
        String filePath = logPath + "/calcite.log";
        log.info("get log:" + filePath);
        LogEventCache logCache = LogEventCache.logCache;
        logCache.clear();

        rollLog(filePath, logCache);
        log.info("logs size:" + logCache.size());
        getLog(logPath);
    }

    private static void rollLog(String filePath, LogEventCache logCache) {
        FileReader fr;
        try {
            fr = new FileReader(filePath);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        try (BufferedReader br = new BufferedReader(fr)) {
            String line;
            long start = System.currentTimeMillis();
            while (!Thread.interrupted()) {
                if ((line = br.readLine()) != null) {
                    logCache.put(line, line);
                } else {
                    Thread.sleep(1000L);
                }
                long end = System.currentTimeMillis();
                if (end - start > 3600000) {
                    break;
                }
            }
        } catch (IOException | InterruptedException e) {
            log.info(e.getMessage(), e);
        } finally {
            try {
                fr.close();
            } catch (IOException ignored) {
            }
        }
    }

    public static void getEvent(String logPath) {
        String filePath = logPath + "/metaEvent.log";
        log.info("get event:" + filePath);
        LogEventCache eventCache = LogEventCache.eventCache;
        eventCache.clear();
        rollLog(filePath, eventCache);
        log.info("event size:" + eventCache.size());
        getEvent(logPath);
    }

    public static CoordinatorServiceConnector getCoordinatorServiceConnector(String coordinator) {
        return io.dingodb.sdk.common.utils.Optional.ofNullable(coordinator.split(","))
            .map(Arrays::stream)
            .map(ss -> ss
                .map(s -> s.split(":"))
                .map(__ -> new io.dingodb.sdk.common.Location(__[0], Integer.parseInt(__[1])))
                .collect(Collectors.toSet()))
            .map(CoordinatorServiceConnector::new)
            .orElseThrow("Create coordinator service connector error.");
    }
}
