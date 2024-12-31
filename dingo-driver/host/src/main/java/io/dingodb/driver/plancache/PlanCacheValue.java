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

package io.dingodb.driver.plancache;

import io.dingodb.exec.base.Job;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.type.SqlTypeName;

import java.sql.Timestamp;
import java.util.List;

@Getter
@Builder
public class PlanCacheValue {
    private String sqlDigest;
    private String sqlText;
    private String stmtType; // select, update, insert, delete, etc.
    private String parseUser; // the user who parses/compiles this plan.
    private String binding; // the binding of this plan.
    private String optimizerEnvHash; // other environment information that might affect the plan like "time_zone", "sql_mode".
    private String parseValues; // the actual values used when parsing/compiling this plan.
    private String planDigest; // digest of the plan, used to identify the plan in the cache.
    private String binaryPlan; // binary of this Plan, use tidb_decode_binary_plan to decode this.
    private long memory; // the memory usage of this plan, in bytes.
    private Timestamp loadTime; // the time when this plan is loaded into the cache.
    private RelNode plan; // READ-WRITE for Session Cache.
    private List<String> outputColumns; // output column names of this plan
    private List<SqlTypeName> paramTypes; // all parameters' types, different parameters may share same plan
    private List<String> stmtHints; // related hints of this plan, like 'max_execution_time'.

    private long executions; // the execution times.
    private long totalKeys; // the total number of returned keys in TiKV.
    private long sumLatency; // the total latency of this plan, in nanoseconds.
    private long lastUsedTimeInUnix; // the last time when this plan is used, in Unix timestamp.
    private boolean isValid;

}
