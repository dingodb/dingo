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

package io.dingodb.index.api;

import io.dingodb.common.codec.KeyValueCodec;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.codec.DingoKeyValueCodec;
import io.dingodb.common.table.Index;
import io.dingodb.common.table.TableDefinition;

import java.util.List;
import java.util.Map;

public abstract class TableCoordinatorServerApi {

    int state = 0;

    public abstract TableDefinition getTableDefinition(String tableName);

    public abstract String getMainExecutorAddr(String tableName);
    public abstract Map<String, String> getAllExecutorAddrs(String tableName);
    public abstract String getIndexExecutorAddr(String tableName, String indexName);

    public boolean addIndex(String tableName, String indexName, String[] columnNames, boolean unique) throws Exception {
        if (state == 1) {
            synchronized (this) {
                // 1. 本地raft获取tableDefinition
                TableDefinition tableDefinition = getTableDefinition(tableName);

                // 2. 验证是否存在同名index
                Index index = tableDefinition.getIndex(indexName);
                if (index != null) {
                    //提示用户同名index已存在
                    return false;
                }

                // 3. 验证是否满足column要求，即tablecolumn包含所有indexcolumn
                for (String columnName : columnNames) {
                    if (tableDefinition.getColumn(columnName) == null) {
                        //提示用户indexcolumn不在tablecolumn中
                        return false;
                    }
                }

                // 4. 验证是否满足unique要求
                if (unique) {
                    //判断是否满足
                    //不满足则返回false，并提示用户
                }

                // 5. 更新tableDefinition，添加index，标记该index状态为busy，并提交到raft
                Index newIndex = new Index(indexName, columnNames, unique);
                tableDefinition.addIndex(newIndex);
                tableDefinition.setIndexBusy(indexName);
                tableDefinition.increaseVersion();
                //TODO: save tabledefinition

            }

            // 6. 新建executor raft group
            //TODO:

            // 7. 启动线程，读取主键索引，插入新索引
            buildIndex(tableName, indexName);

            // 8. 返回
            return true;
        }
        return false;
    }

    public void buildIndex(String tableName, String indexName) throws Exception {
        // 1. 本地获取主键索引地址
        // 2. 获取主键索引对应的executorServerApi
        ExecutorServerApi executorServerApi = null;
        // 3. 修改主键索引中的tableDefinitionVersion
        // 4. 读取所有已完成主键数据， 插入新索引中
        List<KeyValue> records = executorServerApi.getAllFinishedRecord();
        TableDefinition definition = getTableDefinition(tableName);
        KeyValueCodec codec = new DingoKeyValueCodec(definition.getDingoType(), definition.getKeyMapping());
        KeyValueCodec indexCodec = null;
        for (KeyValue record : records) {
            Object[] line = codec.decode(record);
            KeyValue indexRecord = indexCodec.encode(line);
            executorServerApi.insertIndex(indexRecord);
            executorServerApi.insertFinishedRecord(record.getKey(), definition.getVersion());
        }

        // 5. 标记该索引状态为normal
        definition.setIndexNormal(indexName);
        //TODO: save tabledefinition
    }

    public void deleteIndex(String tableName, String indexName) {
        if (state == 1) {
            synchronized (this) {
                TableDefinition definition = getTableDefinition(tableName);
                definition.setIndexDeleted(indexName);
                definition.increaseVersion();
                //TODO: save tabledefinition
            }
            stopBuildIndex(tableName, indexName);
            //TODO: delete index raft group

            synchronized (this) {
                TableDefinition definition = getTableDefinition(tableName);
                definition.deleteIndex(indexName);
                definition.increaseVersion();
                //TODO: save tabledefinition
            }
        }
    }

    public abstract void stopBuildIndex(String tableName, String indexName);

    public abstract Map<String, String> getAllIndexExecutorAddr(String tableName);

    public abstract String getExecutorAddr(String tableName);


    void reboot(String tableName) throws Exception {
        // 1. 状态设置为启动中，即所有服务不可用状态
        state = 0;

        // 2. 等待非delete状态的所有index节点启动完成
        while (getTableDefinition(tableName).getNonDeleteIndexesCount() != 0) {

        }

        // 3. 获取所有未完成主键数据
        getExecutorAddr(tableName);
        ExecutorServerApi executorServerApi = null;
        List<KeyValue> records = executorServerApi.getAllUnfinishedRecord();

        // update 维护
        // 4. 根据未完成主键查询是否存在已完成主键
        List<KeyValue> finishedRecourd = executorServerApi.getFinishedRecord(records);

        // 5. 分类： 未完成主键中没有已完成主键， 未完成主键中有已完成主键且数据一致， 未完成主键中有已完成主键且数据不一致

        List<KeyValue> recordsNonFinished = null;
        List<KeyValue> recordsFinished = null;
        List<KeyValue> recordsUpdated = null;


        // 4. 插入所有index数据
        TableDefinition definition = getTableDefinition(tableName);
        KeyValueCodec codec = new DingoKeyValueCodec(definition.getDingoType(), definition.getKeyMapping());
        for (String indexName : definition.getIndexes().keySet()) {
            KeyValueCodec indexCodec = null;
            for (KeyValue record : recordsNonFinished) {
                Object[] line = codec.decode(record);
                KeyValue indexRecord = indexCodec.encode(line);
                executorServerApi.insertIndex(indexRecord);
            }
            // 5. 标记该索引状态为normal
            definition.setIndexNormal(indexName);
        }
        for (KeyValue record : recordsNonFinished) {
            executorServerApi.insertFinishedRecord(record.getKey(), definition.getVersion());
        }
        for (KeyValue record : recordsNonFinished) {
            executorServerApi.deleteUnfinishedRecord(record.getKey());
        }
        //TODO: save tabledefinition



        // 直接删除已完成且数据没变化的未完成主键
        for (KeyValue record : recordsFinished) {
            executorServerApi.deleteUnfinishedRecord(record.getKey());
        }

        // update数据 同dml update


        // 获取所有标记为删除的主键数据
        List<KeyValue> deleteRecords = executorServerApi.getAllDeleteRecord();

        // 根据主键数据删除所有对应的index
        for (String indexName : definition.getIndexes().keySet()) {
            KeyValueCodec indexCodec = null;
            ExecutorServerApi indexExecutorServerApi = null;
            for (KeyValue record : deleteRecords) {
                Object[] line = codec.decode(record);
                KeyValue indexRecord = indexCodec.encode(line);
                indexExecutorServerApi.deleteIndex(indexRecord);
            }
        }

        // 删除所有标记为删除的主键数据
        for (KeyValue record : deleteRecords) {
            executorServerApi.getFinishedKey(record.getKey());
        }

        // 6. 状态设置为正常状态
        state = 1;

        // 7. 启动心跳发送给coordinator
        // sendheartbeat to coordinator

        // 8. 获取所有busy状态index，查询已完成主键，插入busy状态index
        List<String> busyIndexNames = definition.getBusyIndexes();
        records = executorServerApi.getAllFinishedRecord();

        for (String indexName : busyIndexNames) {
            KeyValueCodec indexCodec = null;
            for (KeyValue record : records) {
                Object[] line = codec.decode(record);
                KeyValue indexRecord = indexCodec.encode(line);
                executorServerApi.insertIndex(indexRecord);
            }
        }

        // 9. 删除标记未delete的index
        List<String> deleteIndexNames = definition.getDeletedIndexes();
        for (String indexName : deleteIndexNames) {
            deleteIndex(tableName, indexName);
            synchronized (this) {
                definition.deleteIndex(indexName);
                definition.increaseVersion();
            }
        }

    }

    public abstract Map<String, String> getIndexExecutorAddr(String tableName, List<String> changedIndexName);
}
