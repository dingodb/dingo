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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.dingodb.common.meta.SchemaInfo;
import io.dingodb.common.meta.SchemaState;
import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.common.table.TableDefinition;
import lombok.Builder;
import lombok.Data;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Data
public class DdlJob {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private long id;
    private ActionType actionType;
    private long schemaId;
    private long tableId;
    private String schemaName;
    private String tableName;
    private JobState state;
    private String warning;
    private String error;
    private long errorCount;
    private long rowCount;
    ReentrantReadWriteLock lock;
    private SchemaState schemaState;
    private long snapshotVer;
    private long realStartTs;
    private long startTs;
    private long dependencyId;
    private String query;
    private long version;
    private String connId;

    private byte[] rawArgs;
    private DdlReorgMeta reorgMeta;
    private long lockVerTs;

    @JsonIgnore
    private List<Object> args;

    private MultiSchemaInfo multiSchemaInfo;

    private int priority;
    private long seqNu;

    @Builder
    public DdlJob(
        long id,
        ActionType actionType,
        long schemaId,
        long tableId,
        String schemaName,
        String tableName,
        JobState state,
        String warning,
        String error,
        long errorCount,
        long rowCount, ReentrantReadWriteLock lock,
        SchemaState schemaState, long snapshotVer, long realStartTs,
        long startTs, long dependencyId, String query, long version, int priority, long seqNu,
        List<Object> args
    ) {
        this.id = id;
        this.actionType = actionType;
        this.schemaId = schemaId;
        this.tableId = tableId;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.state = state;
        this.warning = warning;
        this.error = error;
        this.errorCount = errorCount;
        this.rowCount = rowCount;
        this.lock = lock;
        this.schemaState = schemaState;
        this.snapshotVer = snapshotVer;
        this.realStartTs = realStartTs;
        this.startTs = startTs;
        this.dependencyId = dependencyId;
        this.query = query;
        this.version = version;
        this.priority = priority;
        this.seqNu = seqNu;
        this.args = args;
    }

    public DdlJob() {
    }

    public void setSchemaStateNumber(int number) {
        this.schemaState = SchemaState.get(number);
    }

    public boolean notStarted() {
        return this.getState() == JobState.jobStateDone || this.getState() == JobState.jobStateQueueing;
    }

    public boolean mayNeedReorg() {
        if (actionType == ActionType.ActionAddIndex) {
            return true;
        }
        return false;
    }

    public String job2SchemaIDs() {
        return job2UniqueIDs(true);
    }

    public String job2TableIDs() {
        return job2UniqueIDs(false);
    }

    public String job2UniqueIDs(boolean schema) {
        if (actionType == ActionType.ActionTruncateTable) {
            return tableId + "," + args.get(0);
        }
        if (schema) {
            return String.valueOf(schemaId);
        }
        return String.valueOf(tableId);
    }

    @JsonIgnore
    public boolean isRunning() {
        return state == JobState.jobStateRunning;
    }

    @JsonIgnore
    public boolean isRollingback() {
        return state == JobState.jobStateRollingback;
    }

    @JsonIgnore
    public boolean isDone() {
        return state == JobState.jobStateDone;
    }

    @JsonIgnore
    public boolean isRollbackDone() {
        return state == JobState.jobStateRollbackDone;
    }

    @JsonIgnore
    public boolean isCancelling() {
        return state == JobState.jobStateCancelling;
    }

    @JsonIgnore
    public boolean isCancelled() {
        return state == JobState.jobStateCancelled;
    }

    @JsonIgnore
    public boolean isFinished() {
        return state == JobState.jobStateDone || state == JobState.jobStateRollbackDone || state == JobState.jobStateCancelled;
    }

    @JsonIgnore
    public boolean isSynced() {
        return state == JobState.jobStateSynced;
    }

    public void finishDBJob(JobState jobState, SchemaState schemaState, long version, SchemaInfo schemaInfo) {
        this.state = jobState;
        this.schemaState = schemaState;
    }

    public void finishTableJob(JobState jobState, SchemaState schemaState) {
        this.state = jobState;
        this.schemaState = schemaState;
    }

    public byte[] encode(boolean updateRawArgs) {
        if (updateRawArgs) {
            this.rawArgs = getBytesFromObj(args);
        }
        return getBytesFromObj(this);
    }

    public static byte[] getBytesFromObj(Object arg) {
        if (arg == null) {
            return null;
        }
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        try {
            objectMapper.writeValue(outputStream, arg);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return outputStream.toByteArray();
    }

    public String decodeArgs() {
        try {
            if (rawArgs == null) {
                return null;
            }
            TypeReference t = null;
            if (actionType == ActionType.ActionCreateTable) {
                t = new TypeReference<List<TableDefinition>>() {};
            } else if (actionType == ActionType.ActionCreateSchema) {
                t = new TypeReference<List<SchemaInfo>>() {};
            } else if (actionType == ActionType.ActionTruncateTable) {
                t = new TypeReference<List<Long>>() {};
            } else if (actionType == ActionType.ActionAddIndex) {
                t = new TypeReference<List<TableDefinition>>() {};
            } else if (actionType == ActionType.ActionDropIndex) {
                t = new TypeReference<List<String>>() {};
            } else if (actionType == ActionType.ActionDropColumn) {
                t = new TypeReference<List<String>>() {};
            } else if (actionType == ActionType.ActionAddColumn) {
                t = new TypeReference<List<ColumnDefinition>>() {};
            }

            this.args = (List<Object>) objectMapper.readValue(rawArgs, t);
            return null;
        } catch (Exception e) {
            return e.getMessage();
        }
    }

    @JsonIgnore
    public boolean isRollbackable() {
        switch (this.actionType) {
            case ActionDropIndex:
                if (this.schemaState == SchemaState.SCHEMA_DELETE_ONLY
                    || this.schemaState == SchemaState.SCHEMA_DELETE_REORG
                    || this.schemaState == SchemaState.SCHEMA_WRITE_ONLY) {
                    return false;
                }
                break;
            case ActionDropSchema:
            case ActionDropTable:
            case ActionDropColumn:
                return schemaState == SchemaState.SCHEMA_PUBLIC;
        }
        return true;
    }

    public void addErrorCount(int nu) {
        this.errorCount += nu;
    }

    @Override
    public String toString() {
        return "DdlJob{" +
            "id=" + id +
            ", actionType=" + actionType +
            ", schemaId=" + schemaId +
            ", tableId=" + tableId +
            ", schemaName='" + schemaName + '\'' +
            ", tableName='" + tableName + '\'' +
            ", state=" + state +
            ", warning='" + warning + '\'' +
            ", error='" + error + '\'' +
            ", errorCount=" + errorCount +
            ", rowCount=" + rowCount +
            ", lock=" + lock +
            ", schemaState=" + schemaState +
            ", snapshotVer=" + snapshotVer +
            ", realStartTs=" + realStartTs +
            ", startTs=" + startTs +
            ", dependencyId=" + dependencyId +
            ", query='" + query + '\'' +
            ", version=" + version +
            ", args=" + args +
            ", multiSchemaInfo=" + multiSchemaInfo +
            ", priority=" + priority +
            ", seqNu=" + seqNu +
            '}';
    }
}
