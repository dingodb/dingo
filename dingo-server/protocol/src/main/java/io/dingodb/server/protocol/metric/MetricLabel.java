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

package io.dingodb.server.protocol.metric;

import java.util.Arrays;
import java.util.List;

public class MetricLabel {

    public static final String TABLE = "table";
    public static final String PART = "part";
    public static final String EXECUTOR = "executor";
    public static final String INTERVAL = "interval";

    public static final List<String> PART_LABELS = Arrays.asList(
        TABLE,
        PART,
        EXECUTOR
    );

    public static final List<String> PART_INTERVAL_LABELS = Arrays.asList(
        PART,
        INTERVAL
    );
}
