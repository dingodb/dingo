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

package io.dingodb.transaction.api;

import io.dingodb.common.tenant.TenantConstant;

import java.util.concurrent.atomic.AtomicBoolean;

import static io.dingodb.common.mysql.InformationSchemaConstant.GLOBAL_VAR_PREFIX_BEGIN;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class GcApi {
    public static final int PHYSICAL_SHIFT = 18;
    public static final String lockKeyStr =  "safe_point_update_" + TenantConstant.TENANT_ID;

    public static final String enableKeyStr = "enable_safe_point_update";
    public static final String txnDurationKeyStr = "txn_history_duration";

    public static final AtomicBoolean running = new AtomicBoolean(false);
}
