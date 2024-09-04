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

package io.dingodb.meta.ddl;

import io.dingodb.common.CommonId;
import io.dingodb.common.ddl.ActionType;
import io.dingodb.common.ddl.AffectedOption;
import io.dingodb.common.ddl.SchemaDiff;
import io.dingodb.common.ddl.TableInfoCache;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.meta.SchemaInfo;
import io.dingodb.common.util.Pair;
import io.dingodb.common.util.Utils;
import io.dingodb.meta.InfoSchemaService;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.entity.InfoSchema;
import io.dingodb.meta.entity.SchemaTables;
import io.dingodb.meta.entity.Table;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Slf4j
public class InfoSchemaBuilder {

    InfoSchema is;

    private static final int bucketCount = 100;

    public InfoSchemaBuilder() {
        //is = new InfoSchema();
    }

    public void initWithOldInfoSchema(InfoSchema oldSchema) {
        if (is == null) {
            is = new InfoSchema();
        }
        this.is.schemaMetaVersion = oldSchema.schemaMetaVersion;
        this.is.schemaMap = deepCopy(oldSchema.schemaMap);
        this.is.sortedTablesBuckets = deepCopyBuckets(oldSchema.getSortedTablesBuckets());
    }

    public static Map<String, SchemaTables> deepCopy(Map<String, SchemaTables> original) {
        if (original == null) return null;
        Map<String, SchemaTables> copy = new ConcurrentHashMap<>();
        for (Map.Entry<String, SchemaTables> entry : original.entrySet()) {
            copy.put(entry.getKey(), entry.getValue().copy());
        }
        return copy;
    }

    public static Map<Integer, List<TableInfoCache>> deepCopyBuckets(
        Map<Integer, List<TableInfoCache>> old
    ) {
        Map<Integer, List<TableInfoCache>> copy = new ConcurrentHashMap<>();
        for (Map.Entry<Integer, List<TableInfoCache>> entry : old.entrySet()) {
            List<TableInfoCache> newList = entry.getValue()
                .stream()
                .map(TableInfoCache::deepCopy)
                .collect(Collectors.toList());
            copy.put(entry.getKey(), newList);
        }
        return copy;
    }

    public void initWithSchemaInfos(List<SchemaInfo> schemaInfos, long schemaVersion, InfoSchemaService infoSchemaService) {
        if (is == null) {
            is = new InfoSchema();
        }
        this.is.schemaMetaVersion = schemaVersion;
        for (SchemaInfo schemaInfo : schemaInfos) {
            createSchemaTablesForDB(schemaInfo, infoSchemaService);
        }
    }

    public void createSchemaTablesForDB(SchemaInfo schemaInfo, InfoSchemaService infoSchemaService) {
        Map<String, Table> tableMap = infoSchemaService.listTableDef(schemaInfo.getSchemaId());
        SchemaTables schemaTables = new SchemaTables(schemaInfo, tableMap);
        is.getSchemaMap().put(schemaInfo.getName(), schemaTables);

        tableMap.values().forEach(t -> {
            int idx = bucketIdx(t.tableId.seq);
            is.sortedTablesBuckets.computeIfAbsent(idx, k -> new ArrayList<>());
            is.sortedTablesBuckets.computeIfPresent(idx, (k, v) -> {
                TableInfoCache tableInfo = new TableInfoCache(t.tableId.seq, t.name, schemaInfo.getSchemaId(), schemaInfo.getName());
                if (!v.contains(tableInfo)) {
                    v.add(new TableInfoCache(t.tableId.seq, t.name, schemaInfo.getSchemaId(), schemaInfo.getName()));
                }
                return v;
            });
        });
    }

    public void initWithSchemaInfosByTenant(
        List<SchemaInfo> schemaInfos,
        long schemaVersion,
        InfoSchemaService infoSchemaService,
        long tenantId
    ) {
        if (is == null) {
            is = new InfoSchema();
        }
        this.is.schemaMetaVersion = schemaVersion;
        for (SchemaInfo schemaInfo : schemaInfos) {
            createSchemaTablesForDBByTenant(schemaInfo, infoSchemaService, tenantId);
        }
    }

    public void createSchemaTablesForDBByTenant(
        SchemaInfo schemaInfo,
        InfoSchemaService infoSchemaService,
        long tenantId
    ) {
        Map<String, Table> tableMap = infoSchemaService.listTableDef(schemaInfo.getSchemaId(), tenantId);
        SchemaTables schemaTables = new SchemaTables(schemaInfo, tableMap);
        is.getSchemaMap().put(schemaInfo.getName(), schemaTables);

        tableMap.values().forEach(t -> {
            int idx = bucketIdx(t.tableId.seq);
            is.sortedTablesBuckets.computeIfAbsent(idx, k -> new ArrayList<>());
            is.sortedTablesBuckets.computeIfPresent(idx, (k, v) -> {
                TableInfoCache tableInfo = new TableInfoCache(t.tableId.seq, t.name, schemaInfo.getSchemaId(), schemaInfo.getName());
                if (!v.contains(tableInfo)) {
                    v.add(new TableInfoCache(t.tableId.seq, t.name, schemaInfo.getSchemaId(), schemaInfo.getName()));
                }
                return v;
            });
        });
    }

    public Pair<List<Long>, String> applyDiff(InfoSchemaService infoSchemaService, SchemaDiff schemaDiff) {
        this.is.schemaMetaVersion = schemaDiff.getVersion();
        switch (schemaDiff.getType()) {
            case ActionCreateSchema:
                String error = applyCreateSchema(schemaDiff);
                return Pair.of(null, error);
            case ActionCreateTables:
                return applyCreateTables(infoSchemaService, schemaDiff);
            case ActionAddIndex:
                return applyAddIndex(schemaDiff);
            case ActionDropSchema:
                return applyDropSchema(schemaDiff);
            case ActionDropTable:
                return applyDropTable(schemaDiff);
            case ActionDropIndex:
                return applyDropIndex(schemaDiff);
            case ActionCreateTable:
                return applyCreateTable(schemaDiff);
            case ActionTruncateTable:
                return applyTruncateTable(schemaDiff);
            case ActionDropColumn:
                return applyDropColumn(schemaDiff);
            case ActionAddColumn:
                return applyAddColumn(schemaDiff);
            default:
                break;
        }
        return null;
    }

    public String applyCreateSchema(SchemaDiff diff) {
        int retry = 0;
        SchemaInfo schemaInfo = null;
        InfoSchemaService schemaService = InfoSchemaService.root();
        while (retry ++ < 10) {
            schemaInfo = (SchemaInfo) schemaService.getSchema(diff.getSchemaId());
            if (schemaInfo != null) {
                break;
            }
            Utils.sleep(500);
        }
        if (schemaInfo == null) {
            return "schemaId not exists, schemaId:" + diff.getSchemaId();
        }
        this.is.schemaMap.put(schemaInfo.getName(), new SchemaTables(schemaInfo));
        return null;
    }

    public Pair<List<Long>, String> applyCreateTables(InfoSchemaService infoSchemaService, SchemaDiff diff) {
        if (diff.getAffectedOpts() == null) {
            return Pair.of(null, null);
        }
        List<Long> tableIdList = new ArrayList<>();
        for (AffectedOption affectedOption : diff.getAffectedOpts()) {
            SchemaDiff optDiff = new SchemaDiff(diff.getVersion(), ActionType.ActionCreateTable,
                affectedOption.getSchemaId(),
                affectedOption.getTableId(), affectedOption.getSchemaId(), affectedOption.getOldTableId(),
                false, null);
            Pair<List<Long>, String> optTableIds = applyDiff(infoSchemaService, optDiff);
            if (optTableIds.getValue() != null) {
                return optTableIds;
            }
            tableIdList.addAll(optTableIds.getKey());
        }
        return Pair.of(tableIdList, null);
    }

    public Pair<List<Long>, String> applyCreateTable(SchemaDiff diff) {
        try {
            InfoSchemaService schemaService = InfoSchemaService.root();
            Table table;
            if (diff.getTableId() != 0) {
                table = schemaService.getTableDef(diff.getSchemaId(), diff.getTableId());
            } else {
                table = schemaService.getTableDef(diff.getSchemaId(), diff.getTableName());
            }
            if (table == null) {
                return Pair.of(new ArrayList<>(), null);
            }
            SchemaInfo schemaInfo = (SchemaInfo) schemaService.getSchema(diff.getSchemaId());
            this.is.putTable(schemaInfo.getName(), table.name, table);
            int idx = bucketIdx(diff.getTableId());
            TableInfoCache tmp = new TableInfoCache(table.tableId.seq, table.name, schemaInfo.getSchemaId(), schemaInfo.getName());
            if (is.sortedTablesBuckets.containsKey(idx)) {
                List<TableInfoCache> buckets = is.sortedTablesBuckets.get(idx);
                buckets.add(tmp);
            } else {
                List<TableInfoCache> buckets = new ArrayList<>();
                buckets.add(tmp);
                is.sortedTablesBuckets.put(idx, buckets);
            }
            List<Long> tableIdList = new ArrayList<>();
            tableIdList.add(table.tableId.seq);
            return Pair.of(tableIdList, null);
        } catch (Exception e) {
            LogUtils.error(log, e.getMessage(), e);
            return Pair.of(null, e.getMessage());
        }
    }

    public Pair<List<Long>, String> applyDropTable(SchemaDiff diff) {
        try {
            dropTable(diff.getSchemaId(), diff.getTableId());

            List<Long> tableIdList = new ArrayList<>();
            tableIdList.add(diff.getTableId());
            return Pair.of(tableIdList, null);
        } catch (Exception e) {
            LogUtils.error(log, e.getMessage(), e);
            return Pair.of(null, e.getMessage());
        }
    }

    public boolean dropTable(long schemaId, long tableId) {
        int idx = bucketIdx(tableId);
        List<TableInfoCache> buckets = this.is.sortedTablesBuckets.get(idx);
        if (buckets == null) {
            return false;
        }
        TableInfoCache tableInfo
            = buckets.stream().filter(t -> t.getTableId() == tableId).findFirst().orElse(null);
        if (tableInfo == null) {
            return false;
        }

        SchemaInfo schemaInfo = getSchemaInfo(schemaId);
        if (schemaInfo == null) {
            return false;
        }
        this.is.dropTable(schemaInfo.getName(), tableInfo.getName());
        buckets.remove(tableInfo);
        return true;
    }

    public Pair<List<Long>, String> applyDropSchema(SchemaDiff diff) {
        try {
            SchemaTables schemaTables = this.is.schemaMap.values().stream()
                .filter(s -> s.getSchemaInfo().getSchemaId() == diff.getSchemaId())
                .findFirst().orElse(null);
            if (schemaTables == null) {
                return Pair.of(null, null);
            }
            this.is.schemaMap.remove(schemaTables.getSchemaInfo().getName());

            return Pair.of(null, null);
        } catch (Exception e) {
            return Pair.of(null, e.getMessage());
        }
    }

    public Pair<List<Long>, String> applyAddIndex(SchemaDiff diff) {
        try {
            dropTable(diff.getSchemaId(), diff.getOldTableId());
            return applyCreateTable(diff);
        } catch (Exception e) {
            return Pair.of(null, e.getMessage());
        }
    }

    public Pair<List<Long>, String> applyDropIndex(SchemaDiff diff) {
        try {
            dropTable(diff.getSchemaId(), diff.getOldTableId());
            return applyCreateTable(diff);
        } catch (Exception e) {
            return Pair.of(null, e.getMessage());
        }
    }

    public Pair<List<Long>, String> applyTruncateTable(SchemaDiff diff) {
        try {
            //dropTable(diff.getSchemaId(), diff.getOldTableId());
            return applyCreateTable(diff);
        } catch (Exception e) {
            return Pair.of(null, e.getMessage());
        }
    }

    public Pair<List<Long>, String> applyDropColumn(SchemaDiff diff) {
        try {
            dropTable(diff.getSchemaId(), diff.getTableId());
            MetaService.root()
                .invalidateDistribution(new CommonId(CommonId.CommonType.TABLE, diff.getSchemaId(), diff.getTableId()));
            return applyCreateTable(diff);
        } catch (Exception e) {
            return Pair.of(null, e.getMessage());
        }
    }

    public Pair<List<Long>, String> applyAddColumn(SchemaDiff diff) {
        try {
            dropTable(diff.getSchemaId(), diff.getTableId());
            MetaService.root()
                .invalidateDistribution(new CommonId(CommonId.CommonType.TABLE, diff.getSchemaId(), diff.getTableId()));
            return applyCreateTable(diff);
        } catch (Exception e) {
            return Pair.of(null, e.getMessage());
        }
    }

    public static int bucketIdx(long tableId) {
        return (int) (tableId % bucketCount);
    }

    public SchemaInfo getSchemaInfo(long schemaId) {
        SchemaTables schemaTables = this.is.schemaMap.values().stream()
            .filter(s -> s.getSchemaInfo().getSchemaId() == schemaId)
            .findFirst().orElse(null);
        if (schemaTables == null) {
            return null;
        }
        return schemaTables.getSchemaInfo();
    }

    public InfoSchema build() {
        return is;
    }
}
