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

package io.dingodb.server.executor.ddl;

import io.dingodb.common.ddl.DdlJob;
import io.dingodb.common.ddl.JobState;
import io.dingodb.common.meta.SchemaState;
import io.dingodb.common.util.Pair;
import io.dingodb.sdk.service.entity.meta.TableDefinitionWithId;

import java.util.ArrayList;
import java.util.List;

public final class RollingBackUtil {
    private RollingBackUtil() {

    }

    public static Pair<Long, String> convertAddIdxJob2RollbackJob(
        DdlContext dc,
        DdlJob ddlJob,
        TableDefinitionWithId indexInfo
    ) {
//        List<Object> args = new ArrayList<>();
//        args.add(indexInfo.getTableDefinition().getName());
//        args.add("false");
//        ddlJob.setArgs(args);
        io.dingodb.sdk.service.entity.common.SchemaState originalState = indexInfo.getTableDefinition().getSchemaState();
        indexInfo.getTableDefinition().setSchemaState(io.dingodb.sdk.service.entity.common.SchemaState.SCHEMA_DELETE_ONLY);
        ddlJob.setSchemaState(SchemaState.SCHEMA_DELETE_ONLY);
        Pair<Long, String> res = TableUtil.updateVersionAndIndexInfos(dc, ddlJob, indexInfo, originalState != indexInfo.getTableDefinition().getSchemaState());
        if (res.getValue() != null) {
            return res;
        }
        ddlJob.setState(JobState.jobStateRollingback);
        return Pair.of(res.getKey(), null);
    }
}
