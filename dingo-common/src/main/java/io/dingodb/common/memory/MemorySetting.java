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

package io.dingodb.common.memory;

public class MemorySetting {
    public static final double DEFAULT_GLOBAL_POOL_PROPORTION = 0.70;
    public static final long USE_DEFAULT_MEMORY_LIMIT_VALUE = -1L;
    public static final long UNLIMITED_SIZE = Long.MAX_VALUE;

    public static boolean ENABLE_MEMORY_POOL = true;
    public static boolean ENABLE_MEMORY_LIMITATION = false;
    public static boolean ENABLE_PARSER_POOL = false;
    public static boolean ENABLE_PLANNER_POOL = false;
    public static boolean ENABLE_PLAN_BUILDER_POOL = true;
    public static boolean ENABLE_PLAN_EXECUTOR_POOL = false;
    public static boolean ENABLE_OPERATOR_TMP_TABLE_POOL = true;

    /**
     * 全局spill开关, 同SystemConfig.enableSpill
     */
    public static boolean ENABLE_SPILL = true;

    public static boolean ENABLE_KILL = false;

    /**
     * ap限流开关， 当达到AP高水位线时，限流
     */
    public static boolean ENABLE_LIMIT_APRATE = false;

    /**
     * TP,AP memoryPool的高低水位线配置
     */
    public static double TP_LOW_MEMORY_PROPORTION = 0.20;
    //默认先不开启 0.80
    public static double TP_HIGH_MEMORY_PROPORTION = 1.00;
    public static double AP_LOW_MEMORY_PROPORTION = 0.20;
    //默认先不开启 0.70
    public static double AP_HIGH_MEMORY_PROPORTION = 1.00;

    public static double PROCEDURE_CACHE_LIMIT = 0.3;

    public static double FUNCTION_CACHE_LIMIT = 0.3;

    /**
     * 设置查询最大使用内存比例
     */
    public static final double DEFAULT_ONE_QUERY_MAX_MEMORY_PROPORTION = 0.50;

    private static final double DEFAULT_MPP_GLOBAL_MEMORY_LIMIT_RATIO = 1.0;
    private static double globalMemoryLimitRatio = DEFAULT_MPP_GLOBAL_MEMORY_LIMIT_RATIO;

    public static double getGlobalMemoryLimitRatio() {
        return globalMemoryLimitRatio;
    }

    public static final long DEFAULT_ALLOCATOR_SIZE = 1L << 17;

    public static final long DEFAULT_LESS_REVOKE_BYTES = 32 * (1L << 20);

    public static final Double DEFAULT_MEMORY_REVOKING_THRESHOLD = 0.85;

    public static final Double DEFAULT_MEMORY_REVOKING_TARGET = 0.75;

    private MemorySetting() {
    }
}
