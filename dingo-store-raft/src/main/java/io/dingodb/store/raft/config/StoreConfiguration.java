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

package io.dingodb.store.raft.config;

import io.dingodb.common.config.DingoConfiguration;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class StoreConfiguration {

    private static final StoreConfiguration INSTANCE;

    static {
        try {
            DingoConfiguration.instance().setStore(StoreConfiguration.class);
            INSTANCE = DingoConfiguration.instance().getStore();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static StoreConfiguration instance() {
        return INSTANCE;
    }

    private String dbPath;
    private String dbRocksOptionsFile;
    private String logRocksOptionsFile;
    private int collectStatsInterval = -1;
    private int approximateCount = 10_000;
    private RaftConfiguration raft;
    private boolean collectMetric = false;   // default false, current collect is invalid

    public static Integer collectStatsInterval() {
        return INSTANCE.collectStatsInterval;
    }

    public static String dbPath() {
        return INSTANCE.dbPath;
    }

    public static String dbRocksOptionsFile() {
        return INSTANCE.dbRocksOptionsFile;
    }

    public static String logRocksOptionsFile() {
        return INSTANCE.logRocksOptionsFile;
    }

    public static RaftConfiguration raft() {
        return INSTANCE.raft;
    }

    public static int approximateCount() {
        return INSTANCE.approximateCount;
    }

    public static boolean collectMetric() {
        return INSTANCE.collectMetric;
    }
}
