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

package io.dingodb.common.mysql.scope;

import io.dingodb.common.metrics.DingoMetrics;
import io.dingodb.common.util.Utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public final class ScopeVariables {

    public static final Properties executorProp = new Properties();

    public static final List<String> immutableVariables = new ArrayList<>();

    public static final List<String> globalVariables = List.of("job_need_gc", "txn_history_duration",
        "safepoint_ts", "ssl_enable", "log_bin_trust_function_creators", "innodb_online_alter_log_max_size",
        "max_allowed_packet", "table_definition_cache");

    static {
        //immutableVariables.add("version_comment");
        //immutableVariables.add("version");
        immutableVariables.add("version_compile_os");
        immutableVariables.add("version_compile_machine");
        immutableVariables.add("license");
        immutableVariables.add("default_storage_engine");
        immutableVariables.add("have_openssl");
        immutableVariables.add("have_ssl");
        immutableVariables.add("have_statement_timeout");
        immutableVariables.add("last_insert_id");
        immutableVariables.add("@begin_transaction");

        executorProp.put("transaction_stream_scan", "on");
        executorProp.put("run_ddl", "on");
        executorProp.put("dingo_trx_max_digest_length", "1024");
        executorProp.put("lookup_batch_get_size", "4096");
        executorProp.put("lookup_batch_get", "on");
        executorProp.put("scan_concurrency", "5");
        executorProp.put("mysql_stream_size", "100000");
        executorProp.put("lookup_concurrency", "100");
        executorProp.put("rpc_batch_size", "40960");
        executorProp.put("stats_default_count", "10000");
        executorProp.put("request_factor", "15000");
        executorProp.put("ddl_timeout", "180000");
    }

    private ScopeVariables() {
    }

    public static synchronized Properties putAllGlobalVar(Map<String, String> globalVariableMap) {
        if (globalVariableMap.containsKey("metric_log_enable")) {
            String metricLogEnable = globalVariableMap.get("metric_log_enable");
            metricReporter(metricLogEnable);
        }
        Properties globalVariables = new Properties();
        globalVariables.putAll(globalVariableMap);
        return globalVariables;
    }

    public static synchronized void metricReporter(String metricLogEnable) {
        if ("on".equalsIgnoreCase(metricLogEnable)) {
            DingoMetrics.startReporter();
        } else if ("off".equalsIgnoreCase(metricLogEnable)) {
            DingoMetrics.stopReporter();
        }
    }

    public static boolean containsGlobalVariable(String key) {
        return globalVariables.contains(key);
    }

    public static Integer getRpcBatchSize() {
        String rpcBatchSize = executorProp.getOrDefault("rpc_batch_size", "40960").toString();
        try {
            return Integer.parseInt(rpcBatchSize);
        } catch (Exception e) {
            return 40960;
        }
    }

    public static Double getStatsDefaultCount() {
        String statsDefaultCount = executorProp.getOrDefault("stats_default_count", "10000").toString();
        try {
            return Double.parseDouble(statsDefaultCount);
        } catch (Exception e) {
            return 10000D;
        }
    }

    public static Double getRequestFactor() {
        String requestFactor = executorProp.getOrDefault("request_factor", "15000").toString();
        try {
            return Double.parseDouble(requestFactor);
        } catch (Exception e) {
            return 15000D;
        }
    }

    public static boolean runDdl() {
        String runDdl = executorProp.getOrDefault("run_ddl", "on").toString();
        return runDdl.equalsIgnoreCase("on");
    }

    /**
     * enable txnScan via stream or not.
     * @return streamScan
     */
    public static boolean txnScanByStream() {
        String txnScanByStream = executorProp.getOrDefault("transaction_stream_scan", "on").toString();
        return txnScanByStream.equalsIgnoreCase("on");
    }

    /**
     * Set max length of sql digests for per transaction record in dingo_trx result.
     * The default value is 1024 bytes.
     * @return The max length of sql digests for per transaction record in dingo_trx result.
     */
    public static int getDingoTrxMaxDigestLength() {
        try {
            String maxCount = executorProp.getOrDefault("dingo_trx_max_digest_length", "1024").toString();
            return Integer.parseInt(maxCount);
        } catch (Exception e) {
            //Default value as 1024.
            return 1024;
        }
    }

    public static long getDdlWaitTimeout() {
        try {
            String timeoutStr = executorProp.getOrDefault("ddl_timeout", "180000").toString();
            return Long.parseLong(timeoutStr);
        } catch (Exception e) {
            return 180000;
        }
    }

    public static boolean transaction1Pc() {
        String transaction1Pc = executorProp.getOrDefault("transaction_1pc", "on").toString();
        return transaction1Pc.equalsIgnoreCase("on");
    }

    public static boolean getJob2Table() {
        try {
            String job2Table = executorProp.getOrDefault("job2table", "on").toString();
            return job2Table.equals("on");
        } catch (Exception e) {
            return false;
        }
    }

    public static void testIndexBlock() {
        while (true) {
            String testRun = executorProp.getOrDefault("test_index", "off").toString();
            if (testRun.equalsIgnoreCase("off")) {
                break;
            } else {
                Utils.sleep(1000);
                testRun = executorProp.getOrDefault("test_continue", "off").toString();
                if (testRun.equalsIgnoreCase("on")) {
                    executorProp.setProperty("test_continue", "off");
                    break;
                }
            }
        }
    }

    public static int getDefaultReplica() {
        try {
            String replica = executorProp.getOrDefault("default_replica", "3").toString();
            return Integer.parseInt(replica);
        } catch (Exception e) {
            return 3;
        }
    }

    public static int getSeekFactor() {
        try {
            String seekFactor = executorProp.getOrDefault("seek_factor", "80").toString();
            return Integer.parseInt(seekFactor);
        } catch (Exception e) {
            return 80;
        }
    }

    public static int getLookupConcurrency() {
        try {
            String lookupConcurrency = executorProp.getOrDefault("lookup_concurrency", "100").toString();
            return Integer.parseInt(lookupConcurrency);
        } catch (Exception e) {
            return 1;
        }
    }

    public static int getMysqlStreamSize() {
        try {
            String mysqlStreamSize = executorProp.getOrDefault("mysql_stream_size", "1000000").toString();
            return Integer.parseInt(mysqlStreamSize);
        } catch (Exception e) {
            return 1000000;
        }
    }

    public static int getScanConcurrency() {
        try {
            String scanConcurrency = executorProp.getOrDefault("scan_concurrency", "5").toString();
            return Integer.parseInt(scanConcurrency);
        } catch (Exception e) {
            return 5;
        }
    }

    public static boolean lookupBatchGet() {
        try {
            String lookupBatchGet = executorProp.getOrDefault("lookup_batch_get", "on").toString();
            return "on".equalsIgnoreCase(lookupBatchGet);
        } catch (Exception e) {
            return true;
        }
    }

    public static int lookupBatchSize() {
        try {
            String lookupBatchGetSize = executorProp.getOrDefault("lookup_batch_get_size", "4096").toString();
            return Integer.parseInt(lookupBatchGetSize);
        } catch (Exception e) {
            return 4096;
        }
    }

    public static synchronized void setExecutorProp(String key, String val) {
        if ("rpc_batch_size".equalsIgnoreCase(key)) {
            int rpcBatchSize = Integer.parseInt(val);
            executorProp.put(key, rpcBatchSize);
            return;
        } else if ("stats_default_size".equalsIgnoreCase(key)) {
            double statsDefaultSize = Double.parseDouble(val);
            executorProp.put(key, statsDefaultSize);
            return;
        } else if ("request_factor".equalsIgnoreCase(key)) {
            double requestFactor = Double.parseDouble(val);
            executorProp.put(key, requestFactor);
            return;
        }
        executorProp.put(key, val);
    }
}
