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

package io.dingodb.common.ddl;

import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.common.tenant.TenantConstant;
import org.checkerframework.checker.units.qual.A;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

public final class DdlUtil {
    public static AtomicBoolean timeOutError = new AtomicBoolean(false);
    public static final String DDLGlobalSchemaVersion = "/dingo/ddl/global_schema_version";
    public static final String DDLExpSchemaVersion = "/dingo/ddl/exp_schema_version";
    public static final String DDLAllSchemaVersionsByJob = "/dingo/ddl/all_schema_by_job_versions";
    public static final String DDLAllSchemaVersions = "/dingo/ddl/all_schema_versions|0|";
    public static final String DDLAllSchemaVersionsEnd = "/dingo/ddl/all_schema_versions|1|";

    public static final String ADDING_DDL_JOB_CONCURRENT = "/dingo/ddl/add_ddl_job_general";

    public static final String MDL_PREFIX_TEMPLATE = "%s:%s:%d:|0|";
    public static final String MDL_PREFIX_TEMPLATE_END = "%s:%s:%d:|1|";

    public static final String MDL_TEMPLATE = "%s:%s:%d:|0|-%s";
    public static final String ALL_SCHEMA_VER_SYNC_NORMAL_TEMPLATE = "%s:%s:%s";

    public static final byte[] indexElementKey = "_idx_".getBytes();
    public static final byte[] addColElementKey = "_addCol_".getBytes();
    public static final String tenantPrefix = String.format("tenant:%d", TenantConstant.TENANT_ID);
    public static final String ADDING_DDL_JOB_CONCURRENT_KEY = String.format("%s:%s", tenantPrefix, ADDING_DDL_JOB_CONCURRENT);

    public static boolean mdlEnable = true;

    public static int errorCountLimit = 5;

    public static final String ddlId = String.format("%s:%d", DingoConfiguration.host(), DingoConfiguration.port());

    private DdlUtil() {
    }
}
