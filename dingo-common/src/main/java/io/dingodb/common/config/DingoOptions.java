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

package io.dingodb.common.config;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Getter
@Setter
@ToString
public class DingoOptions {
    private ClusterOptions clusterOpts;
    private String ip;
    private ExchangeOptions exchange;
    private GroupServerOptions coordOptions;
    private ClOptions cliOptions;
    private int queueCapacity = 100;

    private static final DingoOptions INSTANCE = new DingoOptions();

    public static DingoOptions instance() {
        return INSTANCE;
    }
}
