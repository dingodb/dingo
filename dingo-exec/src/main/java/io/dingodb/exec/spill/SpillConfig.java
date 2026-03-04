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

package io.dingodb.exec.spill;

/**
 * Configuration knobs for the spill-to-disk subsystem.
 *
 * <p>All values can be overridden via JVM system properties at startup:
 * <ul>
 *   <li>{@code dingo.spill.dir}            – local directory for spill files
 *       (default: {@code ${java.io.tmpdir}/dingo-spill})</li>
 *   <li>{@code dingo.spill.sort.threshold} – number of in-memory rows before
 *       {@link io.dingodb.exec.operator.SortOperator} spills to disk
 *       (default: {@value #DEFAULT_SORT_SPILL_THRESHOLD})</li>
 *   <li>{@code dingo.spill.agg.threshold}  – number of in-memory groups before
 *       {@link io.dingodb.exec.aggregate.AggCache} spills to disk
 *       (default: {@value #DEFAULT_AGG_SPILL_THRESHOLD})</li>
 *   <li>{@code dingo.spill.join.threshold} – number of build-side tuples before
 *       {@link io.dingodb.exec.operator.HashJoinOperator} spills to disk
 *       (default: {@value #DEFAULT_JOIN_SPILL_THRESHOLD})</li>
 * </ul>
 */
public final class SpillConfig {

    public static final int DEFAULT_SORT_SPILL_THRESHOLD = 100_000;
    public static final int DEFAULT_AGG_SPILL_THRESHOLD  = 100_000;
    public static final int DEFAULT_JOIN_SPILL_THRESHOLD = 100_000;

    private static final String DEFAULT_SPILL_DIR =
        System.getProperty("java.io.tmpdir") + "/dingo-spill";

    // Read once at class-load time so that they can be set in tests.
    private static volatile String spillDir =
        System.getProperty("dingo.spill.dir", DEFAULT_SPILL_DIR);

    private static volatile int sortSpillThreshold =
        Integer.getInteger("dingo.spill.sort.threshold", DEFAULT_SORT_SPILL_THRESHOLD);

    private static volatile int aggSpillThreshold =
        Integer.getInteger("dingo.spill.agg.threshold", DEFAULT_AGG_SPILL_THRESHOLD);

    private static volatile int joinSpillThreshold =
        Integer.getInteger("dingo.spill.join.threshold", DEFAULT_JOIN_SPILL_THRESHOLD);

    private SpillConfig() {
    }

    public static String getSpillDir() {
        return spillDir;
    }

    public static void setSpillDir(String dir) {
        spillDir = dir;
    }

    public static int getSortSpillThreshold() {
        return sortSpillThreshold;
    }

    public static void setSortSpillThreshold(int threshold) {
        sortSpillThreshold = threshold;
    }

    public static int getAggSpillThreshold() {
        return aggSpillThreshold;
    }

    public static void setAggSpillThreshold(int threshold) {
        aggSpillThreshold = threshold;
    }

    public static int getJoinSpillThreshold() {
        return joinSpillThreshold;
    }

    public static void setJoinSpillThreshold(int threshold) {
        joinSpillThreshold = threshold;
    }
}
