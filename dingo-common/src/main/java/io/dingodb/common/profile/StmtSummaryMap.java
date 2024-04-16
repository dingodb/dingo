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

package io.dingodb.common.profile;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.dingodb.common.concurrent.Executors;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

@Slf4j
public final class StmtSummaryMap {
    static BlockingQueue<SqlProfile> profileQueue;
    private static LoadingCache<String, StmtSummary> stmtSummaryMap;
//    private static Map<String, StmtSummary> stmtSummaryHisMap = new ConcurrentHashMap<String, StmtSummary>();

    private StmtSummaryMap() {
    }

    public static Iterator<Object[]> iterator() {
        return stmtSummaryMap.asMap()
        .values()
        .stream()
        .map(StmtSummary::getTuple)
        .iterator();
    }

   static {
        stmtSummaryMap = CacheBuilder.newBuilder()
            .maximumSize(4096)
            .expireAfterAccess(10, TimeUnit.HOURS)
            .expireAfterWrite(10, TimeUnit.HOURS)
//            .removalListener(t -> {
//                stmtSummaryHisMap.put((String) t.getKey(), (StmtSummary) t.getValue());
//            })
            .build(new CacheLoader<String, StmtSummary>() {
                @Override
                public @NonNull StmtSummary load(@NonNull String summaryKey) {
                    return new StmtSummary(summaryKey);
                }
            });
        profileQueue = new LinkedBlockingDeque<>();
        Executors.execute("stmtSummary", StmtSummaryMap::handleProfile);
    }

    private static void handleProfile() {
        while (true) {
            try {
                SqlProfile profile = profileQueue.take();
                summary(profile);
            } catch (InterruptedException ignored) {
            }
        }
    }

    private static void summary(SqlProfile sqlProfile) {
        try {
            StmtSummary stmtSummary = stmtSummaryMap.get(sqlProfile.summaryKey());
            stmtSummary.addSqlProfile(sqlProfile);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public static void addSqlProfile(SqlProfile sqlProfile, Connection connection) {
        boolean slowQueryEnabled = false;
        long slowQueryThreshold = 5000;
        try {
            String enable = connection.getClientInfo("sql_profile_enable");
            if ("off".equals(enable)) {
                return;
            }
            String slowQueryEnableStr = connection.getClientInfo("slow_query_enable");
            slowQueryEnabled = "on".equals(slowQueryEnableStr);
            String slowQueryThresholdStr = connection.getClientInfo("slow_query_threshold");
            if (slowQueryThresholdStr != null) {
                slowQueryThreshold = Long.parseLong(slowQueryThresholdStr);
            }
        } catch (SQLException ignored) {
            log.error(ignored.getMessage(), ignored);
        }
        if (sqlProfile == null) {
            return;
        }
        sqlProfile.end();
        if (slowQueryEnabled && sqlProfile.getDuration() > slowQueryThreshold) {
            log.info("slow query:" + sqlProfile.dumpTree());
        }
        try {
            profileQueue.add(sqlProfile);
            sqlProfile.clear();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

}
