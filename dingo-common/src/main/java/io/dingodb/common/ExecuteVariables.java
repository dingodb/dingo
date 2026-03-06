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

package io.dingodb.common;

import io.dingodb.common.util.Optional;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Properties;

@Getter
@NoArgsConstructor
public class ExecuteVariables {
    public final static int CONCURRENCY_COUNT = 5;
    protected boolean isJoinConcurrency = false;
    protected boolean isExecutorShuffle = false;
    protected int concurrencyLevel = CONCURRENCY_COUNT;
    protected boolean isInsertCheckInplace = false;
    protected int iterationLimit;
    @Setter
    protected String user;
    @Setter
    protected String host;

    protected ExecuteVariables(Properties properties) {
        this.iterationLimit = getIterationLimit(properties);
        this.isJoinConcurrency = isJoinConcurrency(properties);
        this.concurrencyLevel = getConcurrencyLevel(properties);
        this.isInsertCheckInplace = isInsertCheckInplace(properties);
        this.isExecutorShuffle = isExecutorShuffle(properties);
    }

    public int getConcurrencyLevel(Properties properties) {
        Optional<String> concurrencyLevelOpt = Optional.ofNullable(
            properties.getProperty("dingo_partition_execute_concurrency"));
        return concurrencyLevelOpt
            .map(Integer::parseInt)
            .orElse(5);
    }

    public int getIterationLimit(Properties properties) {
        Optional<String> concurrencyLevelOpt = Optional.ofNullable(
            properties.getProperty("cte_max_recursion_depth"));
        return concurrencyLevelOpt
            .map(Integer::parseInt)
            .orElse(1000);
    }

    public boolean isJoinConcurrency(Properties properties) {
        return "on".equalsIgnoreCase(properties.getProperty("dingo_join_concurrency_enable"));
    }

    public boolean isInsertCheckInplace(Properties properties) {
        return "on".equalsIgnoreCase(properties.getProperty("dingo_constraint_check_in_place"));
    }

    public boolean isExecutorShuffle(Properties properties) {
        return "on".equalsIgnoreCase(properties.getProperty("dingo_execute_shuffle_enable"));
    }

    public String getUser() {
        return user;
    }

    public String getHost() {
        return host;
    }
}
